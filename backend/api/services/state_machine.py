class StateMachine:
    def __init__(self, indicator_loader):
        self.indicator_loader = indicator_loader

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
                weight = indicator_params['weight']
                weighted_sum += result * weight
                total_weight += weight

        # Calculate the weighted average score
        if total_weight > 0:
            weighted_score = weighted_sum / total_weight
            print(f"Weighted Score for {tier}: {weighted_score}")  # Debugging line
            return weighted_score
        return 0

    def evaluate_state(self, weighted_score, threshold=0.7):
        """
        Evaluate if the current state is Green or Red based on the weighted score.
        :param weighted_score: The weighted score for the tier.
        :param threshold: Threshold above which the state is considered Green.
        :return: 'Green' if score exceeds the threshold, otherwise 'Red'.
        """
        return 'Green' if weighted_score >= threshold else 'Red'

    def run_state_machine(self, indicator_results_by_tier):
        """
        Run the state machine logic across all tiers (macro, daily, micro).
        :param indicator_results_by_tier: A dictionary of {tier: {indicator_name: result}}.
        :return: State for each tier.
        """
        states = {}
        for tier, indicator_results in indicator_results_by_tier.items():
            weighted_score = self.calculate_weighted_score(indicator_results, tier)
            state = self.evaluate_state(weighted_score)
            states[tier] = state
        return states
