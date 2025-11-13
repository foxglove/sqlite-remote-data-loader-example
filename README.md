# connector-upstream-test

This is an example connector that reads and streams back local files. It returns a manifest that returns sources of 10MB or so chunks to provide better streaming performance.

## Running

Put MCAPs in the `files` directory and then run the server. Query the manifest endpoint like so: `/manifest?name=<my file in files>.mcap`.
