def largest_non_adjacent(array):
    if len(array) == 1:
        return array[0]
    if len(array) == 2:
        return max(array)
    else:
        return max([array[-1], largest_non_adjacent(array[:-2])+array[-1], largest_non_adjacent(array[:-1])])

def largest_non_adjacent_it(array):
    solution = [0] * len(array)
    solution[0] = array[0]
    solution[1] = max(array[:1])

    for i, _ in enumerate(array):
            solution[i] = max(array[i],
                              solution[i-2] + array[i],
                              solution[i-1])

    return solution[-1]


test = [-2, 1, 3, -4, 5]
print(largest_non_adjacent(test))
print(largest_non_adjacent_it(test))
