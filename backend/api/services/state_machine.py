# backend/api/services/state_machine.py
from datetime import datetime
from backend.data.repositories._sqlite_db import SQLiteDB

class StateMachine:
    def __init__(self, indicator_loader, db_connection):
        self.current_state = 'YELLOW'
        self.indicator_loader = indicator_loader
        self.db = db_connection

    def calculate_weighted_score(self, indicator_results, tier):
        """
        Calculate the weighted score based on indicator results and weights.
        :param indicator_results: A dictionary of {indicator_name: result} (result is 1 for favorable, 0 for not).
        :param tier: The current analysis tier (macro, daily, micro).
        :return: Weighted score for the tier.
        """
        weighted_sum = 0
        total_weight = 0

        for indicator_name, result in indicator_results.items():
            if indicator_params := self.indicator_loader.get_indicator_params(
                indicator_name, tier
            ):
                weight = indicator_params.get('weight', 1)  # Default to weight 1 if not found
                weighted_sum += result * weight
                total_weight += weight

        # Calculate the weighted average score
        if total_weight > 0:
            weighted_score = weighted_sum / total_weight
            print(f"Weighted Score for {tier}: {weighted_score}")  # Debugging line
            return weighted_score
        return 0

    def can_trade(self, instrument_id):
        """
        Check if the instrument is in the green state across all timeframes.
        """
        monthly_state = self.get_current_state(instrument_id, 'monthly')
        daily_state = self.get_current_state(instrument_id, 'daily')
        minute_state = self.get_current_state(instrument_id, 'minute')

        # Trade only if all states are green
        return monthly_state == 'GREEN' and daily_state == 'GREEN' and minute_state == 'GREEN'

    def transition_to(self, instrument_id, timeframe, new_state):
        """
        Transition to a new state for a specific instrument and timeframe.
        """
        if new_state in ['RED', 'YELLOW', 'GREEN']:
            print(f"Transitioning {timeframe} state for instrument {instrument_id} to {new_state}")
            self.current_state = new_state
            self.update_state_in_db(instrument_id, timeframe, new_state)
        else:
            raise ValueError(f"Invalid state transition: {new_state}")

    def evaluate_state(self, instrument_id, market_conditions, timeframe, weighted_score, threshold=0.7):
        """
        Evaluate market conditions and decide which state to transition to.
        Evaluate if the current state is Green or Red based on the weighted score.
        :param instrument_id: The instrument ID.
        :param market_conditions: A dictionary of market conditions (e.g., volatility, risk level).
        :param timeframe: The timeframe to evaluate (e.g., 'monthly', 'daily', 'minute').
        :param weighted_score: The weighted score for the tier.
        :param threshold: Threshold above which the state is considered Green.
        """
        risk_level = market_conditions.get('risk_level', 0)
        volatility = market_conditions.get('volatility', 0)

        # Determine the state based on weighted score and market conditions
        if (
            weighted_score >= threshold
            and (risk_level > 7 or volatility > 5)
            or weighted_score < threshold
        ):
            self.transition_to(instrument_id, timeframe, 'RED')
        elif 4 < risk_level <= 7:
            self.transition_to(instrument_id, timeframe, 'YELLOW')
        else:
            self.transition_to(instrument_id, timeframe, 'GREEN')

    def get_current_state(self, instrument_name, timeframe):
        """
        Get the current state of the instrument for the specified timeframe.
        """
        instrument_id = self.db.get_instrument_id(instrument_name)
        query_result = self.db.fetch_records(
            "instrument_states",
            {"instrument_id": instrument_id, "timeframe": timeframe},
        )
        
        if query_result and "state" in query_result[0]:
            return query_result[0]["state"]
        return 'UNKNOWN'

    def update_state_in_db(self, instrument_id, timeframe, new_state):
        """
        Update the instrument's state in the database for a specific timeframe.
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
            INSERT INTO instrument_states (instrument_id, timeframe, state, last_updated)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(instrument_id, timeframe) DO UPDATE SET
                state = excluded.state,
                last_updated = excluded.last_updated
        """
        
        SQLiteDB.execute_script(query, (instrument_id, timeframe, new_state, timestamp))

    def run_state_machine(self, instrument_id, indicator_results_by_tier, market_conditions):
        """
        Run the state machine logic across all tiers (macro, daily, micro).
        :param instrument_id: The instrument ID to update in the database.
        :param indicator_results_by_tier: A dictionary of {tier: {indicator_name: result}}.
        :param market_conditions: A dictionary of market conditions (volatility, risk level, etc.)
        :return: State for each tier.
        """
        states = {}
        for tier, indicator_results in indicator_results_by_tier.items():
            weighted_score = self.calculate_weighted_score(indicator_results, tier)
            self.evaluate_state(instrument_id, market_conditions, tier, weighted_score)
            # After evaluation, get the updated state from the database
            state = self.get_current_state(instrument_id, tier)
            states[tier] = state

        return states

# # Initialize the StateMachine with the IndicatorConfigLoader and SQLite connection
# indicator_loader = IndicatorConfigLoader()
# db_connection = SQLiteDB("instruments.db")
# state_machine = StateMachine(indicator_loader, db_connection)
