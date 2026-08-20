# The leaf's own targets. There is no platform here and no emulator: a leaf
# is resolved, read and unit-tested on its own, and a platform is pointed at
# it (see README).
.PHONY: help show-product test

help:
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

show-product: ## Stage the core product's SQL locally and list what it contains
	@uv run python -m contoso_product.show --into product

test: ## Leaf-boundary tests. No Docker, no emulator, no platform.
	uv run --extra dev pytest tests -q
