"""Defines the intermediate representation of redipy scripts. The execution
graph has three components: 1) expressions which output values, 2) statements
which consist mostly of expressions and typically change the execution context
via side-effects, and 3) sequences which group statements."""
