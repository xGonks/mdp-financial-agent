import pandas as pd
from itertools import product

def create_transition_matrix(features, memory):
    if not memory:
        if isinstance(features, pd.DataFrame):
            groups = [features[feature].unique() for feature in features.columns]
        elif isinstance(features, pd.Series):
            groups = [features.unique()]

        states = list(product(*groups))
        print(states)
        for i in states:
            print(len(i))
        matrix = pd.DataFrame(0, index=states, columns=states, dtype=float)






        print("No mem")

    print()



def matrices_transicion(data, col_discreta="close_discreta"):
    estados = data[col_discreta].unique().tolist()
    matriz = pd.DataFrame(0, index=estados, columns=estados, dtype=float)
    for i in range(len(data) - 1):
        estado_actual = data.loc[i, col_discreta]
        estado_siguiente = data.loc[i + 1, col_discreta]
        matriz.loc[estado_actual, estado_siguiente] += 1
        
    matriz = matriz.div(matriz.sum(axis=1), axis=0).fillna(0)
    
    return matriz
