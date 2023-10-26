WEB_VERSION = "0.2.26"

WEB_SURVEY_VERSION = "0.0.1"
WEB_MS-TRACING_VERSION = "0.0.1"
WEB_THEME-CERNBOX_VERSION = "0.2.0"
WEB_IFC-JS_VERSION = "2.0.2"
WEB_TOURS_VERSION = "1.0.0"
WEB_TOURS-CERNBOX_VERSION = "0.0.8"
WEB_ROOTJS_VERSION = "2.0.0"
WEB_TOP-BAR_VERSION = "1.0.0"
WEB_OPEN-IN-SWAN_VERSION = "2.0.0"
WEB_OLD-WEB-REDIRECTOR_VERSION = "1.0.0"
WEB_JUPYTER_VERSION = "2.0.0"

all: clean pull-web pull-web-extensions

clean:
	git clean -xfd assets/

pull-web:
	curl -s -lL -o- "https://github.com/cernbox/web-release/releases/download/v$(WEB_VERSION)/web.tar.gz" | tar -xzf - -C assets/web/

pull-web-extensions:
	mkdir -p assets/web_extensions/survey-$(WEB_SURVEY_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/survey%2Fv$(WEB_SURVEY_VERSION)/survey.tar.gz" | tar -xzf - -C assets/web_extensions/survey-$(WEB_SURVEY_VERSION)/
	mkdir -p assets/web_extensions/ms-tracing-$(WEB_MS-TRACING_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/ms-tracing%2Fv$(WEB_MS-TRACING_VERSION)/ms-tracing.tar.gz" | tar -xzf - -C assets/web_extensions/ms-tracing-$(WEB_MS-TRACING_VERSION)/
	mkdir -p assets/web_extensions/theme-cernbox-$(WEB_THEME-CERNBOX_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/theme-cernbox%2Fv$(WEB_THEME-CERNBOX_VERSION)/theme-cernbox.tar.gz" | tar -xzf - -C assets/web_extensions/theme-cernbox-$(WEB_THEME-CERNBOX_VERSION)/
	mkdir -p assets/web_extensions/ifc-js-$(WEB_IFC-JS_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/ifc-js%2Fv$(WEB_IFC-JS_VERSION)/ifc-js.tar.gz" | tar -xzf - -C assets/web_extensions/ifc-js-$(WEB_IFC-JS_VERSION)/
	mkdir -p assets/web_extensions/tours-$(WEB_TOURS_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/tours%2Fv$(WEB_TOURS_VERSION)/tours.tar.gz" | tar -xzf - -C assets/web_extensions/tours-$(WEB_TOURS_VERSION)/
	mkdir -p assets/web_extensions/tours-cernbox-$(WEB_TOURS-CERNBOX_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/tours-cernbox%2Fv$(WEB_TOURS-CERNBOX_VERSION)/tours-cernbox.tar.gz" | tar -xzf - -C assets/web_extensions/tours-cernbox-$(WEB_TOURS-CERNBOX_VERSION)/
	mkdir -p assets/web_extensions/rootjs-$(WEB_ROOTJS_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/rootjs%2Fv$(WEB_ROOTJS_VERSION)/rootjs.tar.gz" | tar -xzf - -C assets/web_extensions/rootjs-$(WEB_ROOTJS_VERSION)/
	mkdir -p assets/web_extensions/top-bar-$(WEB_TOP-BAR_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/top-bar%2Fv$(WEB_TOP-BAR_VERSION)/top-bar.tar.gz" | tar -xzf - -C assets/web_extensions/top-bar-$(WEB_TOP-BAR_VERSION)/
	mkdir -p assets/web_extensions/open-in-swan-$(WEB_OPEN-IN-SWAN_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/open-in-swan%2Fv$(WEB_OPEN-IN-SWAN_VERSION)/open-in-swan.tar.gz" | tar -xzf - -C assets/web_extensions/open-in-swan-$(WEB_OPEN-IN-SWAN_VERSION)/
	mkdir -p assets/web_extensions/old-web-redirector-$(WEB_OLD-WEB-REDIRECTOR_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/old-web-redirector%2Fv$(WEB_OLD-WEB-REDIRECTOR_VERSION)/old-web-redirector.tar.gz" | tar -xzf - -C assets/web_extensions/old-web-redirector-$(WEB_OLD-WEB-REDIRECTOR_VERSION)/
	mkdir -p assets/web_extensions/jupyter-$(WEB_JUPYTER_VERSION) && curl -s -lL -o- "https://github.com/cernbox/web-extensions/releases/download/jupyter%2Fv$(WEB_JUPYTER_VERSION)/jupyter.tar.gz" | tar -xzf - -C assets/web_extensions/jupyter-$(WEB_JUPYTER_VERSION)/
	
