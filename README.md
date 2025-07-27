# Ferron Python modules

The Ferron modules that provide gateway interfaces utilizing Python.

## Notes

### `ferron-modules-asgi` module

This module runs ASGI applications on a single worker process. Due to Python's GIL (Global Interpreter Lock), the performance might be lower than what it would be run on multiple worker processes.

This module expects the ASGI application to have `application` as the ASGI callback. If you're using some other callback name, you can create the file below (assuming that the callback name is `app` and the main application Python file is `app.py`):

```python
from app import app

application = app
```

This module requires that Ferron links to the Python library.

### `ferron-modules-wsgi` module

This module runs WSGI applications on a single worker process. Due to Python's GIL (Global Interpreter Lock), the performance might be lower than what it would be run on multiple worker processes. If you are using Unix or a Unix-like system, it's recommended to use the `ferron-modules-wsgid` module instead.

This module expects the WSGI application to have `application` as the WSGI callback. If you're using some other callback name, you can create the file below (assuming that the callback name is `app` and the main application Python file is `app.py`):

```python
from app import app

application = app
```

This module requires that Ferron links to the Python library.

### `ferron-module-wsgid` module

This module runs WSGI applications on a pre-forked process pool. This module can be enabled only on Unix and a Unix-like systems. Additionaly, it's recommended to stop the processes in the process pool in addition to the main process, as the server will not automatically stop the processes in the process pool (except on Linux systems, where the processes in the process pool are automatically stopped when the server is stopped).

This module expects the WSGI application to have `application` as the WSGI callback. If you're using some other callback name, you can create the file below (assuming that the callback name is `app` and the main application Python file is `app.py`):

```python
from app import app

application = app
```

This module requires that Ferron links to the Python library.

## Additional KDL configuration directives

### Global-only directives

- `wsgi_clear_imports [wsgi_clear_imports: bool]` (_wsgi_ module)
  - This directive specifies whenever to enable Python module import path clearing. Setting this option as `wsgi_clear_imports #true` improves the compatiblity with setups involving multiple WSGI applications, however module imports inside functions must not be used in the WSGI application. Default: `wsgi_clear_imports #false`
- `asgi_clear_imports [asgi_clear_imports: bool]` (_asgi_ module)
  - This directive specifies whenever to enable Python module import path clearing. Setting this option as `asgi_clear_imports #true` improves the compatiblity with setups involving multiple ASGI applications, however module imports inside functions must not be used in the ASGI application. Default: `asgi_clear_imports #false`

**Configuration example:**

```kdl
* {
    wsgi_clear_imports #false
    asgi_clear_imports #false
}
```

### Directives

- `wsgi <wsgi_application_path: string|null>` (_wsgi_ module)
  - This directive specifies whenever WSGI is enabled and the path to the WSGI application. The WSGI application must have an `application` entry point. Default: `wsgi #null`
- `wsgid <wsgi_application_path: string|null>` (_wsgid_ module)
  - This directive specifies whenever WSGI with pre-forked process pool is enabled and the path to the WSGI application. The WSGI application must have an `application` entry point. Default: `wsgid #null`
- `asgi <asgi_application_path: string|null>` (_asgi_ module)
  - This directive specifies whenever ASGI is enabled and the path to the ASGI application. The ASGI application must have an `application` entry point. Default: `asgi #null`

**Configuration example:**

```kdl
wsgi.example.com {
    // WSGI configuration
    wsgi "/var/www/myapp/app.py"
}

wsgid.example.com {
    // WSGI with daemon mode
    wsgid "/var/www/myapp/app.py"
}

asgi.example.com {
    // ASGI configuration
    asgi "/var/www/myapp/asgi.py"
}
```

## Compiling Ferron with these modules

To compile Ferron with these modules, first clone the Ferron repository:

```bash
git clone https://github.com/ferronweb/ferron.git -b develop-2.x
cd ferron
```

Then, copy the `ferron-build.yaml` file to `ferron-build-override.yaml`:

```bash
cp ferron-build.yaml ferron-build-override.yaml
```

After that, add the following line to the `ferron-build-override.yaml` file:

```yaml
modules:
  # Other modules...
  - git: https://github.com/ferronweb/ferron-modules-python.git
    crate: ferron-module-asgi
    loader: AsgiModuleLoader
  - git: https://github.com/ferronweb/ferron-modules-python.git
    crate: ferron-module-wsgid
    loader: WsgidModuleLoader
  - git: https://github.com/ferronweb/ferron-modules-python.git
    crate: ferron-module-wsgi
    loader: WsgiModuleLoader
```

It's suggested to define these modules between the module with `ReverseProxyModuleLoader` and `FcgiModuleLoader`.

After modifying the `ferron-build-override.yaml` file, you can compile Ferron with this module by running the following command:

```bash
make build
```

You can then package it in a ZIP archive using the following command:

```bash
make package
```

The ZIP archive will be created in the `dist` directory, and can be installed using Ferron installer.
