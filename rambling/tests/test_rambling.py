import time
import pytest
import socket

import rambling

@pytest.yield_fixture
def ramble():
    r = rambling.Rambling(nm_port=8020, rm_port=8088)
    yield r

def test_cmd(ramble):
    cmd = "python -c 'import socket; print(socket.gethostname())'"
    appId = ramble.start_application(cmd)
    
    status = ramble.get_application_status(appId)
    while status['app']['finalStatus'] != 'SUCCEEDED':
        status = ramble.get_application_status(appId)
        time.sleep(2)
    
    hostname = socket.gethostname()
    logs = ramble.get_application_logs(appId, shell=True)
    print(logs)
     
    assert hostname in logs
