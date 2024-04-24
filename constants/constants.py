class Template:
    TEMP = 'TEMP'

class Table:
    MAIN = '[DWH].[MAIN]'
    EXECUTION_LOGS = '[DWH].[EXECUTION_LOGS]'

class Status:
    SENT_TO_ORCHESTRATION = 'SENT TO ORCHESTRATION'
    RUNNING = 'RUNNING'
    SUCCESSFULLY_UPLOADED = 'SUCCESSFULLY UPLOADED'
    SUCCESSFULLY_UPLOADED_WITH_WARNINGS = 'SUCCESSFULLY UPLOADED WITH WARNINGS'
    FAILED_TO_UPLOAD = 'FAILED TO UPLOAD'
    INTERNAL_ERROR = 'INTERNAL ERROR'

class Worksheet:
    DEFINITION = 'Definition'

class Error:
    NOT_DEFINED_ERROR = 'NOT DEFINED ERROR'

class ResultAction:
    FAILURE = 'Failure'
    WARNING = 'Warning'

class Log:
    INSERT = 'INSERT'
    REPLACE = 'REPLACE'