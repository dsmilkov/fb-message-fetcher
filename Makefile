PATH := ./node_modules/.bin:${PATH}

init:
	npm install

clean:
	rm -rf lib/ test/*.js

build:
	coffee -o lib/ -c src/ && coffee -c test/*.coffee

dist: clean init build

publish: dist
		npm publish