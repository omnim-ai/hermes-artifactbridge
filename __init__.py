# Hermes directory-plugin entry point (loaded as hermes_plugins.<slug>); the package holds the code.
try:
    from .hermes_artifactbridge import PLUGIN, register  # noqa: F401
except ImportError:  # imported outside the Hermes loader (e.g. as a top-level module)
    from hermes_artifactbridge import PLUGIN, register  # noqa: F401
