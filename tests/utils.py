def is_subset(subset_dict, full_dict):
    return subset_dict == {k: full_dict[k] for k in subset_dict}
