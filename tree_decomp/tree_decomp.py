class Step:
    def __init__(self, id_, direction, threshold: float, feature_id):
        self.id_ = id_
        self.direction = direction
        self.threshold = threshold
        self.feature_id = feature_id

    def __repr__(self):
        return f"{self.id_} {'Left' if self.direction == 1 else 'Right'}"


class Leaf:
    def __init__(self, id_, binary_class: int, class_proba: float):
        self.id_ = id_
        self.binary_class = binary_class
        self.class_proba = class_proba


class Path:
    def __init__(self, leaf: Leaf, steps: list[Step] = None):
        self.steps = steps if steps else []
        self.binary_class = leaf.binary_class
        self.class_proba = leaf.class_proba

    def add_step(self, step: Step, left: bool = False):
        if left:
            self.steps.insert(0, step)
        else:
            self.steps.append(step)

    def is_positive(self) -> bool:
        return bool(self.binary_class)

    def __len__(self):
        return len(self.steps)


class Bound:
    def __init__(self, num_features: int, mins: list[float], maxs: list[float]):
        if len(mins) != len(maxs): raise ValueError("mins and maxs must have the same length")
        if num_features != len(maxs): raise ValueError("num_features must be equal to number of mins/maxs")

        self.mins = mins
        self.maxs = maxs

    def get_bound_as_str(self):
        return " ".join([f"{_min},{_max}" for _min, _max in zip(self.mins, self.maxs)])

    def __str__(self):
        return self.get_bound_as_str()


def get_leaf_paths(tree, node_id=0):
    """
    This will return all the path to all leaf nodes in a Tree object from sklearn using recursion
    Args:
        tree: A sklearn Tree object
        node_id: the current node id (for recursive calls, set to root when initializing)
    Returns:
        paths: a list of Path objects
    """
    left_child = tree.children_left[node_id]
    right_child = tree.children_right[node_id]
    threshold = tree.threshold[node_id]
    feature = tree.feature[node_id]

    if left_child != -1:
        left_paths = get_leaf_paths(tree, left_child)
        right_paths = get_leaf_paths(tree, right_child)

        for path in left_paths:
            path.add_step(Step(node_id, 1, threshold, feature), left=True)
        for path in right_paths:
            path.add_step(Step(node_id, 0, threshold, feature), left=True)
        paths = left_paths + right_paths
    else:
        _value = tree.value[node_id].squeeze()
        paths = [Path(leaf=Leaf(node_id, min(range(len(_value)), key=lambda x: _value[x]), _value[1]/_value.sum()))]
    return paths


def get_bounds_from_path(path, num_features):
    """
    Converts a single Path object into the min,max bounds for each feature to describe the shape of the leaf region
    Args:
        path: a Path object
        num_features: number of features in the training data
    Returns:
        bounds: a list of Bounds objects
    """
    # TODO I think SALSA is capped from -1 to 1, but for now this should cover it
    _mins = [-1000]*num_features
    _maxs = [1000]*num_features

    for step in path.steps:
        if step.direction == 1:
            _maxs[step.feature_id] = step.threshold
        else:
            _mins[step.feature_id] = step.threshold

    return Bound(num_features=num_features, mins=_mins, maxs=_maxs)


def write_bounds_to_file(bounds, filename):
    with open(filename, "w") as f:
        for bound in bounds:
            f.write(f"{str(bound)}\n")


def get_number_of_range_queries_in_tree(tree):
    paths = get_leaf_paths(tree)
    return len([path for path in paths if path[0][4] == 1])


def get_number_of_range_queries_in_forest(forest):
    return sum([get_number_of_range_queries_in_tree(tree.tree_) for tree in forest.estimators_])


def estimate_runtime(num_queries, sec_per_query=30.0):
    return num_queries * sec_per_query


if __name__ == '__main__':
    # for testing
    from sklearn.datasets import make_classification
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier

    X, y = make_classification(n_samples=1000, weights=[0.01, 0.99])
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
    clf = RandomForestClassifier(random_state=0, n_estimators=10, n_jobs=-1)
    clf.fit(X_train, y_train)
    paths = [p for dt in clf.estimators_ for p in get_leaf_paths(dt.tree_)]
    bounds = [get_bounds_from_path(path, 20) for path in paths]
