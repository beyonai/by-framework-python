# pylint: disable=C0114,C0115,C0116
# pytest fixtures and test patterns generate false positives for:
# - redefined-outer-name (W0621): pytest fixtures
# - unused-argument (W0613): test callbacks
# - unused-variable (W0612): test setup variables
# - arguments-renamed (W0237): override patterns
# - redefined-builtin (W0622): pytest setUp methods
# - broad-exception-caught (W0718): test error handling
# pylint: enable=C0114,C0115,C0116
