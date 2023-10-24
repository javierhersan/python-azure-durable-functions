import azure.functions as func
import azure.durable_functions as df
from functions.function import app as bp

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS) 
app.register_functions(bp)
# app.register_blueprint(bp)

