import numpy as np

from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

from tqdm import tqdm

X, y = make_classification(n_samples=500)
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
clf = RandomForestClassifier(random_state=0, n_estimators=10, max_depth=3)
clf.fit(X_train, y_train)

lines = []
tree_cutoff = 0.5
# mode = "raw"
mode = "proba"

LEAF_NODE = -1


# path object goes [node_id, thresh_direction, threshold, feature_id, binary_class, probability of class 1]
# for everything but threshold a value of -1 or -2 is Null
def get_leaf_paths(tree, node_id=0):
    left_child = tree.children_left[node_id]
    right_child = tree.children_right[node_id]
    threshold = tree.threshold[node_id]
    feature = tree.feature[node_id]

    if left_child != -1:
        left_paths = get_leaf_paths(tree, left_child)
        right_paths = get_leaf_paths(tree, right_child)

        for path in left_paths:
            path.append((node_id, 1, threshold, feature, -1, -1))
        for path in right_paths:
            path.append((node_id, 0, threshold, feature, -1, -1))
        paths = left_paths + right_paths
    else:
        _value = tree.value[node_id].squeeze()
        paths = [[(node_id, -1, threshold, feature, np.argmax(_value), _value[1]/_value.sum())]]
    return paths


def get_bounds_from_path(path, num_features):
    _bounds = [[None, None] for _ in range(num_features)]
    for step in path:
        _bounds[step[3]][step[1]] = step[2]
    return _bounds


# for all trees in the forest, get the regions that they claim are active
all_bounds = []
for dt in tqdm(clf.estimators_):
    leaf_paths = get_leaf_paths(dt.tree_)
    positive_leaf_paths = [path[1:][::-1] for path in leaf_paths if path[0][4] == 1]  # reverse order and remove the leaf
    all_bounds.append([get_bounds_from_path(path, clf.n_features_in_) for path in positive_leaf_paths])







    # strings = []
    # for path in paths:
    #     _feat_dict = {feat_idx: [-10, 10] for feat_idx in range(dt.tree_.n_features)}
    #     _leaf_proba = np.nan
    #     _leaf_std = np.nan
    #     _feat = None
    #     _thresh = None
    #     for step in path:
    #         if _feat is not None:
    #             _feat_dict[_feat][step[1]] = min(_feat_dict[_feat][step[1]], _thresh) if step[1] else (
    #                 max(_feat_dict[_feat][step[1]], _thresh))  # 1 for left (max), 0 for right (min)
    #
    #         _feat = feature[step[0]]
    #         _thresh = threshold[step[0]]
    #
    #         if _feat == -2:
    #             if mode == "raw":
    #                 _leaf_proba = values[step[0]][1] / (values[step[0]][0] + values[step[0]][1])
    #                 _leaf_std = 0.0
    #             else:
    #                 samps = np.array([[np.random.uniform(lower, upper)
    #                                    for lower, upper in _feat_dict.values()]
    #                                   for i in range(100)])
    #                 probs = clf.predict_proba(samps)[:, 1]
    #                 _leaf_proba = np.mean(probs)
    #                 _leaf_std = np.std(probs)
    #             break
    #
    #     if _leaf_proba > tree_cutoff:
    #         _str = f"{_leaf_proba},{_leaf_std}"
    #         for feat_idx, (lower, upper) in _feat_dict.items():
    #             _str += f",{feat_idx},{lower},{upper}"
    #         _str += "\n"
    #         strings.append(_str)
    # lines.extend(strings)
    # lines.append("$$$ENDTREE$$$")