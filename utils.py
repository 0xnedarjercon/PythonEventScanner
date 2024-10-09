from multiprocessing.managers import ListProxy, DictProxy

# Override the __repr__ and __str__ methods of the ListProxy class so it can be viewed in debugger
def listproxy_repr(self):
    return repr(list(self))

def listproxy_str(self):
    return str(list(self))
# Assign the new methods to the ListProxy class
ListProxy.__repr__ = listproxy_repr
ListProxy.__str__ = listproxy_str

# extracts the blocks from a job
def blocks(filter):
    try:
        if type(filter) == list or type(filter) == ListProxy:
            #get args from job
            filter = filter[1]
        if type(filter) == tuple:
            #get filter from args
            filter = filter[0]
        #return the blocks
        return (filter['fromBlock'], filter['toBlock'], filter['toBlock']-filter['fromBlock'])
    except:
        return 'not get_logs'

# converts multiprocessing types to native types for easier debugging
def toNative(obj):
    # Recursively convert multiprocessing Manager lists and dicts to normal Python types
    if isinstance(obj, ListProxy):
        return [toNative(item) for item in obj]
    elif isinstance(obj, DictProxy):
        return {key: toNative(value) for key, value in obj.items()}
    else:
        return obj