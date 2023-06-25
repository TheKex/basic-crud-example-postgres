
class DbConnectionError(Exception):
    pass


class DbTableAlreadyExists(Exception):
    def __init__(self, table_name, message=None):
        self.table_name = table_name
        self.message = message or f'Table "{self.table_name}" is already exists'
        super().__init__(self.message)


class DbTableIsNotExists(Exception):
    def __init__(self, table_name, message=None):
        self.table_name = table_name
        self.message = message or f'Table "{self.table_name}" is not exists'
        super().__init__(self.message)


