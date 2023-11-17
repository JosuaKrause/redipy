help:
	@echo "The following make targets are available:"
	@echo "install	install all python dependencies"
	@echo "lint-comment	ensures fixme comments are grepable"
	@echo "lint-emptyinit	main inits must be empty"
	@echo "lint-flake8	run flake8 checker to deteck missing trailing comma"
	@echo "lint-forgottenformat	ensures format strings are used"
	@echo "lint-indent	run indent format check"
	@echo "lint-pycodestyle	run linter check using pycodestyle standard"
	@echo "lint-pycodestyle-debug	run linter in debug mode"
	@echo "lint-pylint	run linter check using pylint standard"
	@echo "lint-requirements	run requirements check"
	@echo "lint-stringformat	run string format check"
	@echo "lint-type-check	run type check"
	@echo "lint-all	run all lints"
	@echo "compileall 	compile all python scripts"
	@echo "pre-commit 	sort python package imports using isort"
	@echo "name	generate a unique permanent name for the current commit"
	@echo "git-check	ensures no git visible files have been altered"
	@echo "git-check-publish	ensures no git visible files have been altered"
	@echo "pack	build pypi packages"
	@echo "publish	build and publish pypi packages"
	@echo "clean	clean test and build data"
	@echo "pytest	run all test with pytest (requires a running test redis server)"
	@echo "pytest-ga	run tests for github actions only with pytest (requires a running test redis server)"
	@echo "split-test	run test according to the previous result file with pytest"
	@echo "requirements-check	check whether the env differs from the requirements file"
	@echo "requirements-complete	check whether the requirements file is complete"
	@echo "run-redis-test	start redis server for pytest"
	@echo "coverage-report	show the coverage report for python"
	@echo "stubgen	create stubs for a package"
	@echo "allapps	find all python entry points"
	@echo "version	prints the current version from pyproject.toml"
	@echo "version-tag	prints the current version from the last deployed tag"
	@echo "version-next	prints the next version after the last deployed tag (to be used in pyproject.toml)"

export LC_ALL=C
export LANG=C

PYTHON=python

lint-comment:
	! ./sh/findpy.sh \
	| xargs --no-run-if-empty grep --color=always -nE \
	  '#.*(todo|xxx|fixme|n[oO][tT][eE]:|Note:|nopep8\s*$$)|.\"^s%'

lint-emptyinit:
	[ ! -s app/__init__.py ]

lint-stringformat:
	! ./sh/findpy.sh \
	| xargs --no-run-if-empty grep --color=always -nE "%[^'\"]*\"\\s*%\\s*"

lint-indent:
	! ./sh/findpy.sh \
	| xargs --no-run-if-empty grep --color=always -nE "^(\s{4})*\s{1,3}\S.*$$"

lint-forgottenformat:
	! PYTHON=$(PYTHON) ./sh/forgottenformat.sh

lint-requirements:
	locale
	cat requirements.txt
	sort -ufc requirements.txt

lint-pycodestyle:
	./sh/findpy.sh | sort
	./sh/findpy.sh | sort | xargs --no-run-if-empty pycodestyle --show-source

lint-pycodestyle-debug:
	./sh/findpy.sh | sort
	./sh/findpy.sh \
	| sort | xargs --no-run-if-empty pycodestyle -v --show-source

lint-pylint:
	./sh/findpy.sh | sort
	./sh/findpy.sh | sort | xargs --no-run-if-empty pylint -j 6 -v

lint-type-check:
	mypy .

lint-flake8:
	flake8 --verbose --select C812,C815,C816,E303,I001,I002,I003,I004,I005 \
	--exclude venv,.git,.mypy_cache,userdata,ui --show-source ./

lint-all: \
	lint-comment \
	lint-emptyinit \
	lint-stringformat \
	lint-indent \
	lint-forgottenformat \
	lint-requirements \
	requirements-complete \
	lint-pycodestyle \
	lint-pylint \
	lint-type-check \
	lint-flake8

install:
	PYTHON=$(PYTHON) ./sh/install.sh

requirements-check:
	PYTHON=$(PYTHON) ./sh/requirements_check.sh $(FILE)

requirements-complete:
	PYTHON=$(PYTHON) ./sh/requirements_complete.sh $(FILE)

name:
	git describe --abbrev=10 --tags HEAD

git-check:
	./sh/git_check.sh

git-check-publish: git-check
	./sh/git_check.sh

pack: clean
	./sh/pack.sh

publish: clean git-check-publish
	./sh/publish.sh

compileall: clean
	./sh/compileall.sh

pre-commit:
	pre-commit install
	isort .

clean:
	./sh/clean.sh

pytest:
	MAKE=$(MAKE) PYTHON=$(PYTHON) RESULT_FNAME=$(RESULT_FNAME) ./sh/run_pytest.sh $(FILE)

pytest-ga:
	MAKE=$(MAKE) PYTHON=$(PYTHON) RESULT_FNAME=$(RESULT_FNAME) GITHUB_ACTIONS=true ./sh/run_pytest.sh $(FILE)

split-test:
	MAKE=$(MAKE) PYTHON=$(PYTHON) ./sh/split_test.sh

run-redis-test:
	PYTHON=$(PYTHON) PORT=$(PORT) ./sh/run_redis.sh

coverage-report:
	cd coverage/reports/html_report && open index.html

stubgen:
	PYTHON=$(PYTHON) FORCE=$(FORCE) ./sh/stubgen.sh $(PKG)

allapps:
	./sh/findpy.sh \
	| xargs --no-run-if-empty grep '__name__ == "__main__"' \
	| cut -d: -f1 \
	| sed -e 's/^.\///' -e 's/\/__main__.py$$//' -e 's/.py$$//'

version:
	./sh/version.sh

version-tag:
	./sh/version.sh --tag

version-next:
	./sh/version.sh --tag --next
