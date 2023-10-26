WEB_VERSION = "0.2.26"
WEB_SURVEY_VERSION = "0.0.1"

all: clean pull-web pull-web-extensions

clean:
	git clean -xfd assets/

pull-web:
	curl -s -lL -o- "https://github.com/cernbox/web-release/releases/download/v$(WEB_VERSION)/web.tar.gz" | tar -xzf - -C assets/web/

pull-web-extensions:
	mkdir -p assets/web_extensions/survey-$(WEB_SURVEY_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/survey%2Fv$(WEB_SURVEY_VERSION)/survey.tar.gz" | tar -xzf - -C assets/web_extensions/survey-$(WEB_SURVEY_VERSION)/
	
