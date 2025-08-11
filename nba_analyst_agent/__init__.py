"""Analytic agent package.

Avoid importing submodules at package import time to prevent side effects.
"""

# Intentionally do not import submodules here. The deployment environment may not
# have all dependencies available during metadata inspection.
from . import agent
__all__ = ["agent"]