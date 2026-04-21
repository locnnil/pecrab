# Git Hooks

This repository contains Git hooks that can be used to automate tasks and enforce policies in your Git workflow.
Git hooks are scripts that run automatically at certain points in the Git life-cycle, such as before a commit is made or after a push is completed.

## Installation

To install the Git hooks, follow these steps:

1. Clone this repository to your local machine.
2. Configure `.git-hooks` as the hooks directory for your Git repository by running the following command in your terminal:

```shell
git config core.hooksPath .git-hooks
```

