all: clean pull-web pull-web-extensions

clean:
	git clean -xfd assets/

pull-web:
	curl -lL -o- https://github.com/cernbox/web-release/releases/download/v0.2.26/web.tar.gz | tar -xzf - -C assets/web/

pull-web-extensions:
	echo 2 > assets/web_extensions/echo2
