#### <a name="commit-header"></a>Commit Message Header

```
<type>(<scope>): <short summary>
  │                    │
  │                    └─⫸ Summary in present tense. Not capitalized. No period at the end.
  │       
  |
  │
  └─⫸ Commit Type: build|chore|ci|docs|feat|fix|perf|refactor|style|test
```

The `<type>` and `<summary>` fields are mandatory, the `(<scope>)` field is optional.


##### Type

Must be one of the following:

* **build**: Changes that affect the build system or external dependencies
* **chore**: Changes that do not relate to a fix or feature and don't modify src or test files (for example updating dependencies)
* **ci**: Changes to our CI configuration files and scripts (examples: CircleCi, SauceLabs)
* **docs**: Updates to documentation such as a the README or other markdown files
* **feat**: A new feature
* **fix**: A bug fix
* **perf**: A code change that improves performance
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **style**: Changes that do not affect the meaning of the code, likely related to code formatting such as white-space, missing semi-colons, and so on
* **test**: Adding missing tests or correcting existing tests



##### Summary

Use the summary field to provide a succinct description of the change:

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize the first letter
* no dot (.) at the end
