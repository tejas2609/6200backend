from fastapi import Depends, Query

def as_query(model: type):
    def dependency(**kwargs):
        return model(**kwargs)

    annotations = {
        field: Query(None)
        for field in model.__annotations__.keys()
    }

    dependency.__annotations__ = annotations
    return Depends(dependency)