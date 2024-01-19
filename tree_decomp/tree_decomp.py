import itertools
from numba import njit

import numpy as np

from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

from tqdm import tqdm

X, y = make_classification(n_samples=5000, weights=[0.01, 0.99])
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
clf = RandomForestClassifier(random_state=0, n_estimators=100, n_jobs=-1)
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
    # TODO I think SALSA is capped from -1 to 1, but for now this should cover it
    _bounds = [[-1000, 1000] for _ in range(num_features)]
    for step in path:
        _bounds[step[3]][step[1]] = step[2]
    return _bounds


@njit
def merge_bounds(bound1, bound2, new_bound, num_features):
    for dim_idx in range(num_features):
        dim_1_min = bound1[dim_idx, 0]
        dim_1_max = bound1[dim_idx, 1]

        dim_2_min = bound2[dim_idx, 0]
        dim_2_max = bound2[dim_idx, 1]

        # check if the feature overlaps at all
        # "or equal" checks because boundaries are one-way exclusive, so a point on a boundary is in only one bound
        # not both, thus the bound do no merge to make a square overlap at all.
        if (dim_1_max <= dim_2_min) or (dim_2_max <= dim_1_min):
            return False

        # if got here then that means feature overlaps
        _min = max(dim_1_min, dim_2_min)
        _max = min(dim_1_max, dim_2_max)
        new_bound[dim_idx, 0] = _min
        new_bound[dim_idx, 1] = _max
    return True


def merge_tree_bounds(tree_bounds1, tree_bounds2, num_features):
    new_tree = []
    for bound1 in tree_bounds1:
        for bound2 in tree_bounds2:
            new_bound = np.zeros((num_features, 2), int)
            _check = merge_bounds(np.array(bound1), np.array(bound2), new_bound, num_features)
            if _check:
                new_tree.append(new_bound)
    if len(new_tree) > 0:
        return new_tree
    else:
        return None


def merge_forest_bounds(all_bounds, num_features):
    num_trees = len(all_bounds)
    tree_parents = {i: {i} for i in range(num_trees)}
    tree_combos = list(itertools.combinations(range(num_trees), 2))
    known_overlaps = {i: {i} for i in range(num_trees)}

    _rounds = 0
    while True:
        print(f"round {_rounds} of size {num_trees}")
        new_trees = []
        for tree_idx_1, tree_idx_2 in tqdm(tree_combos):
            new_tree_weight = all_bounds[tree_idx_1][0] + all_bounds[tree_idx_2][0]
            new_tree = merge_tree_bounds(all_bounds[tree_idx_1][1], all_bounds[tree_idx_2][1], num_features)
            if new_tree is None:
                continue
            else:
                known_overlaps[tree_idx_1].add(tree_idx_2)
                known_overlaps[tree_idx_2].add(tree_idx_1)

                num_trees += 1
                new_trees.append(num_trees-1)
                all_bounds.append((new_tree_weight, new_tree))
                tree_parents[num_trees-1] = tree_parents[tree_idx_1].union(tree_parents[tree_idx_2])
                tree_parents[num_trees-1].add(num_trees-1)
                known_overlaps[num_trees - 1] = set(tree_parents[num_trees - 1].copy())

        print("finished merging trees")
        # the failure to create new "trees" means all possible tree mergers have happened, can exit loop
        if len(new_trees) == 0:
            break

        new_tree_combos = []
        for new_tree_idx in tqdm(new_trees):
            possible_trees_to_search = list(range(num_trees))
            # remove parents
            for p in tree_parents[new_tree_idx]:
                possible_trees_to_search.remove(p)
            # remove impossible overlaps.
            # basically, if none of the original parents of a tree overlap, it is impossible
            #  for the two trees to overlap, as all new (sub) trees are a smaller subset of existing trees
            new_tree_overlap = known_overlaps[new_tree_idx]
            for tree_idx in possible_trees_to_search.copy():
                if len(new_tree_overlap.intersection(known_overlaps[tree_idx])) == 0:
                    possible_trees_to_search.remove(tree_idx)
            new_tree_combos.extend([(p_idx, new_tree_idx) for p_idx in possible_trees_to_search])
        tree_combos = new_tree_combos
        _rounds += 1


def decompose_random_forest(forest):
    # for all trees in the forest, get the regions that they claim are active
    all_bounds = []
    for dt in tqdm(forest.estimators_, disable=True):
        leaf_paths = get_leaf_paths(dt.tree_)
        # reverse order and remove the leaf
        positive_leaf_paths = [path[1:][::-1] for path in leaf_paths if path[0][4] == 1]
        # if we want to scale the tree by some weight, the first value of this tuple is where to do that
        all_bounds.append((1, [get_bounds_from_path(path, forest.n_features_in_) for path in positive_leaf_paths]))

    merge_forest_bounds(all_bounds, forest.n_features_in_)
    return all_bounds


res = decompose_random_forest(clf)
