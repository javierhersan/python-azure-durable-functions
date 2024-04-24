import azure.functions as func
import azure.durable_functions as df
from models.Request import Request

app = df.Blueprint() 

# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    # function_name = req.route_params.get('functionName')
    # instance_id = await client.start_new(function_name)
    body = req.get_json()
    instance_id = await client.start_new('hello_orchestrator', client_input=body)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    body = context.get_input()
    request = Request(**body)
    # result1 = yield context.call_activity("hello", "Seattle")
    # result2 = yield context.call_activity("hello", "Tokyo")
    # result3 = yield context.call_activity("hello", "London")

    input = {'param1':"param1", "param2": "param2"}
    output = yield context.call_activity("activity", input)
    return output

    # return [result1, result2, result3]

# Activity
@app.activity_trigger(input_name="city")
def hello(city: str):
    return "Hello " + city

@app.activity_trigger(input_name="input", activity='activity')
def activity(input):
    out = input["param1"]
    return out