output_dir = $(CURDIR)/deploy
MAVEN = mvn


install: all
	mkdir -p $(output_dir)/lib/dependencies $(output_dir)/doc
	cp $(CURDIR)/target/bdglue*.jar $(output_dir)/lib
	cp $(CURDIR)/target/dependencies/*.jar $(output_dir)/lib/dependencies
	cp -R $(CURDIR)/target/apidocs $(output_dir)/doc
	cp $(CURDIR)/*.pdf $(output_dir)/doc

all: bdglue.jar


.PHONY: check-env

bdglue.jar:  .PHONY
	$(MAVEN) package -Dggbd.VERSION=$(GGBD_VERSION) -Dggbd.HOME=$(GGBD_HOME)

clean: .PHONY
	$(MAVEN) clean
	rm -rf $(output_dir)

check-env:
ifndef GGBD_HOME
	@echo *** GGBD_HOME is the directory where GG for Big Data in installed.
	@echo *** For example, if GG for Big Data is installed at /u01/ggbd12_2, then you would set
	@echo *** export GGBD_HOME=/u01/ggbd12_2
	$(error environment variable GGBD_HOME is undefined)
endif
ifndef GGBD_VERSION
	@echo *** GGBD_VERSION is an environment variable set to the version of the ggdbutil-VERSION.jar file
	@echo *** found in the GGBD_HOME/ggjava/resources/lib directory.
	@echo *** For example, if the file is named ggdbutil-12.2.0.1.0.012.jar, then you would set
	@echo *** export GGBD_VERSION=12.2.0.1.0.012
	$(error environement variable GGBD_VERSION is undefined)
endif
