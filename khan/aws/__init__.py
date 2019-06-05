import base64
import importlib
import json
import os
import sys
from threading import Thread


def cloud_thread_handler(event, context):
    thread_name = event.get('name')
    print(f"[H] [{thread_name}] - Cloud Thread starts.")
    target_func = event.get('target_func')
    target_m = event.get('target_module').split('.')

    target_module = importlib.__import__(target_m[0])
    for m in target_m[1:]:
        target_module = getattr(target_module, m)
    # print(target_module)
    func = getattr(target_module, target_func)
    params = json.loads(event.get('params'))
    print(f"[H] [{thread_name}] - Target acquired.")
    result = func(*params)
    print(f"[H] [{thread_name}] - Target run.")
    return result


def deploy_handler(extra_modules=None):
    from .deploy import new_lambda
    from .config import LAMBDA_NAME
    new_lambda(LAMBDA_NAME, cloud_thread_handler, extra_modules)


class LambdaInvoker(object):
    def __init__(self):
        import botocore.session
        import botocore.config
        from .config import LAMBDA_NAME, AWS_REGION
        self.session = botocore.session.get_session()
        self.config = botocore.config.Config(max_pool_connections=200,
                                             read_timeout=600)

        self.region_name = AWS_REGION
        self.lambda_function_name = LAMBDA_NAME
        self.lambda_cli = self.session.create_client('lambda',
                                                     region_name=AWS_REGION,
                                                     config=self.config)
        self.TIME_LIMIT = True

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        return self.lambda_cli.invoke(FunctionName=self.lambda_function_name,
                                      Payload=json.dumps(payload),
                                      InvocationType='RequestResponse',
                                      LogType='Tail')


class CloudThread(Thread):

    def __init__(self, target, *params):
        super().__init__()
        self.name = 'Cloud'+self.name
        self.target = target
        if not callable(self.target):
            raise ValueError("Target is not callable.")
        self.params = params
        self.invoker = LambdaInvoker()
        self.response = None
        self.local = False

    def run(self):
        print(f"[T] [{self.name}] - Cloud thread started.")
        m_name = self.target.__module__
        if m_name == '__main__':
            filename = sys.modules[self.target.__module__].__file__
            m_name = os.path.splitext(os.path.basename(filename))[0]
        payload = {'target_func': self.target.__name__,
                   'target_module': m_name,
                   'params': json.dumps(self.params),
                   'name': self.name}
        # print(payload)
        if self.local:
            print(f"[T] [{self.name}] - Local mode.")
            self.response = cloud_thread_handler(payload, None)
            # print(f"[T] [{self.name}] - Cloud thread completed.")

        else:
            # TODO Control error in function
            invoke_result = self.invoker.invoke(payload)
            # print(f"[T] [{self.name}] - Cloud thread completed.")
            print(f"[T] [{self.name}] - Lambda Tail Log:")
            print(base64.b64decode(invoke_result.get('LogResult'))
                  .decode('utf-8'))
            self.response = json.loads(
                invoke_result.get('Payload').read().decode('utf-8'))
        # print(f"[T] [{self.name}] - Return payload:")
        # print(self.response)
        # print(f"[T] [{self.name}] - Exiting Cloud Thread.")

    def get_response(self):
        return self.response
