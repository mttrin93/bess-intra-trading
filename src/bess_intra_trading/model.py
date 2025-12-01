from pulp import (
    LpProblem, LpVariable, lpSum, LpMaximize, PULP_CBC_CMD, LpStatus
)
import pandas as pd


def solve_intrinsic_problem(
        prices: pd.DataFrame,
        soc_initial: float,
        bess_params: dict,
        solver_params: dict = None
) -> dict:
    """
    Formulates and solves the Mixed-Integer Linear Programming (MILP)
    problem for the intrinsic battery optimization over a fixed time horizon.

    Args:
        prices (pd.DataFrame): DataFrame with index as time, and columns
                                'bid' (selling) and 'ask' (buying) prices.
        soc_initial (float): Battery's State of Charge at the start of the horizon [MWh].
        bess_params (dict): Dictionary containing 'capacity_mwh', 'min_soc',
                            'max_soc', 'efficiency', 'max_power_mw'.
        solver_params (dict): Dictionary for solver settings (e.g., PuLP solver).

    Returns:
        dict: Results including optimal charge, discharge, final SoC, and profit.
    """
    T = len(prices)  # Number of time intervals
    dt = bess_params['time_step_h']  # Time step length in hours (e.g., 0.25h for 15 min)

    # --- 1. Problem Definition ---
    prob = LpProblem("BESS_Intrinsic_Optimization", LpMaximize)

    # --- 2. Decision Variables ---
    # Power variables [MW]
    charge = LpVariable.dicts("Charge", range(T), lowBound=0, upBound=bess_params['max_power_mw'])
    discharge = LpVariable.dicts("Discharge", range(T), lowBound=0, upBound=bess_params['max_power_mw'])

    # Binary variables (optional, but often necessary for BESS constraints like simultaneous charge/discharge)
    # is_charging = LpVariable.dicts("Is_Charging", range(T), cat='Binary')
    # is_discharging = LpVariable.dicts("Is_Discharging", range(T), cat='Binary')

    # State of Charge [MWh]
    soc = LpVariable.dicts("SoC", range(T + 1), lowBound=bess_params['min_soc'], upBound=bess_params['max_soc'])

    # --- 3. Objective Function (Maximize Revenue) ---
    # Revenue = (Discharge * Bid Price - Charge * Ask Price) * dt
    revenue = lpSum([
        discharge[t] * prices.iloc[t]['bid'] * dt - charge[t] * prices.iloc[t]['ask'] * dt
        for t in range(T)
    ])
    prob += revenue, "Total_Revenue"

    # --- 4. Constraints ---
    eff = bess_params['efficiency']
    M = bess_params['max_power_mw']  # Big M constant

    # 4.1. Initial SoC constraint
    prob += soc[0] == soc_initial, "Initial_SoC"

    # 4.2. SoC Balance Equation (Energy transition constraint)
    for t in range(T):
        prob += soc[t + 1] == soc[t] + (charge[t] * eff - discharge[t] / eff) * dt, f"SoC_Balance_{t}"

    # 4.3. Min/Max SoC are defined in variable bounds.
    # 4.4. Power constraints (defined in variable bounds).

    # 4.5. Mutually exclusive charge/discharge (If binary variables are used, enforce them here)
    # The simpler intrinsic model often omits the binaries unless needed for non-linear constraints.
    # If the model requires it: prob += is_charging[t] + is_discharging[t] <= 1, f"No_Simultaneous_Flow_{t}"

    # --- 5. Solve the Problem ---
    solver = PULP_CBC_CMD(**(solver_params or {}))  # Use default CBC or pass custom solver
    prob.solve(solver)

    # --- 6. Extract Results ---
    if LpStatus[prob.status] == "Optimal":
        results = {
            'revenue': prob.objective.value(),
            'final_soc': soc[T].value(),
            'charge': [charge[t].value() for t in range(T)],
            'discharge': [discharge[t].value() for t in range(T)],
            # Only the first period's action matters for the Rolling Intrinsic
            'optimal_action_mw': (discharge[0].value() if discharge[0].value() > 0 else 0) - \
                                 (charge[0].value() if charge[0].value() > 0 else 0)
        }
        return results
    else:
        # Handle infeasibility or non-optimal solution
        return {
            'revenue': 0,
            'final_soc': soc_initial,
            'optimal_action_mw': 0,
            'status': LpStatus[prob.status]
        }
