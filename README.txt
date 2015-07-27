Basic implementation of TeraSort in tez.
This is not complete implementation. Checking in the current codebase for time being.
- Created sample data in local machine and ran the TeraSort job using local mode. Works fine.
- But, needs to be tested in large cluster (need to verify if jars are distributed properly etc).
- For TeraGen and TeraValidate, we can possibly make use of the existing MR codebase with yarn-tez setting.
- Custom comparator, serialization etc needs to be added once this is stabilized.
