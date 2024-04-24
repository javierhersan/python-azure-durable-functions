class DataBaseError(Exception):
    def __init__(self, error, message):            
        super().__init__(message)
        self.error = error
