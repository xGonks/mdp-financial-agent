from pandas import qcut

def discretize_feature(feature, strategy, ngroups, prefix="group"):
    if strategy == "quantile-based":
        return qcut(x=feature,
                    q=ngroups,
                    labels=[f"{prefix} Group {i}" for i in range(1, ngroups + 1)])
    
    elif strategy == "model-based":
        print()
