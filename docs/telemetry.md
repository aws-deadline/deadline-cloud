# Telemetry

This library collects telemetry data by default. Telemetry events contain non-personally-identifiable information that helps us understand how users interact with our software so we know what features our customers use, and/or what existing pain points are.

You can opt out of telemetry data collection by either:

1. Setting the environment variable: `DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true`
2. Setting the config file: `deadline config set telemetry.opt_out true`

Note that setting the environment variable supersedes the config file setting.
