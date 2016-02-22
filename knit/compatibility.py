from __future__ import absolute_import, division, print_function

import sys

if sys.version_info < (3,):
    FileNotFoundError = IOError
    PermissionError = IOError
    from urlparse import urlparse
    
else:
    PermissionError = PermissionError
    FileNotFoundError = FileNotFoundError
    from urllib.parse import urlparse
    
    
try:
    from subprocess import check_output

except ImportError:
    import subprocess
    def check_output(*popenargs, **kwargs):
        """Backported from Python 2.7 as it's implemented as pure python on stdlib."""

        process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            error = subprocess.CalledProcessError(retcode, cmd)
            error.output = output
            raise error
        return output
