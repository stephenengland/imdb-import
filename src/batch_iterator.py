def batch_iterator(iterable, size=100, filter_expression=None):
    current_batch = []

    for x in iterable:
        if filter_expression:
            if filter_expression(x):
                current_batch.append(x)
        else:
            current_batch.append(x)
        
        if len(current_batch) == size:
            yield current_batch
            current_batch = []

    if current_batch:
            yield current_batch