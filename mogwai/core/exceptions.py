class QueryError(Exception):
    def __init__(self, message="Bad Query"):
        self.message = message
        super().__init__(self.message)


class GraphTraversalError(Exception):
    def __init__(self, message="Traversal failed"):
        self.message = message
        super().__init__(self.message)


class MogwaiGraphError(Exception):
    def __init__(self, message="Graph did strange things"):
        self.message = message
        super().__init__(self.message)
