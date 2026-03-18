import pandas as pd
from itertools import product

def create_transition_matrix(features, memory):
    if not memory:
        if isinstance(features, pd.DataFrame):
            groups = [features[feature].unique() for feature in features.columns]
        elif isinstance(features, pd.Series):
            groups = [features.unique()]

        states = list(product(*groups))
        matrix = pd.DataFrame(0, index=states, columns=states, dtype=float)

        for i in range(len(features) - 1):
            if isinstance(features, pd.DataFrame):
                actual_state = tuple(features.loc[i, :])
                next_state = tuple(features.loc[i + 1, :])
            elif isinstance(features, pd.Series):
                actual_state = (features.loc[i],)
                next_state = (features.loc[i + 1],)

            matrix.at[actual_state, next_state] += 1

        matrix = matrix.div(matrix.sum(axis=1), axis=0).fillna(0)

        return matrix
