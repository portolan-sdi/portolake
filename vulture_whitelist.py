# Vulture whitelist — false positives from pytest fixture dependency injection.
# Vulture doesn't understand that these parameters are used by pytest to
# establish fixture dependencies, not by the function body.

docker_services  # used as pytest fixture dependency in e2e/conftest.py
