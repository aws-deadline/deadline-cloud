# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Text printed by `deadline --guide`: a task-oriented guide for AI agents and scripts.

`--help` documents one command at a time. This guide covers what help pages can't:
how commands chain into workflows, how to write a job bundle, and the defaults that
silently do the wrong thing in a non-interactive session. Keep every command and flag
in sync with the real CLI; test/unit/deadline_client/cli/test_cli_guide.py checks them.
"""

AGENT_GUIDE = """\
# deadline CLI guide for agents and scripts

Run `deadline <group> <command> --help` for every flag. This guide covers the
workflows and defaults that help pages don't.

## Concepts

- Farm > queue > job > step > task. A job comes from an Open Job Description
  (OpenJD) template. A step runs one task per combination of its task parameters.
- Fleets of workers run tasks. A session is one worker running tasks of one job,
  inside the queue's and job's environments (setup, the tasks, then cleanup).
- Queue environments (for example Conda) can add job parameters. List them with
  `deadline queue paramdefs`.
- Job attachments copy your input files to S3 on submit and let you download
  outputs afterwards. A storage profile maps paths between operating systems.

## Rules for non-interactive use

- Pass `--yes` to every command that can prompt (`bundle submit`,
  `job download-output`, `job cancel`, `job requeue-tasks`). Without it they
  wait on stdin.
- Pass `--profile`, `--farm-id`, `--queue-id`, and `--job-id` explicitly.
  Omitted values fall back to the defaults in `~/.deadline/config`, which may
  point at someone else's farm. `defaults.aws_profile_name` there overrides
  the AWS_PROFILE environment variable.
- `deadline config set` changes that shared file for the user. Prefer flags.
- Commands with `--output` print JSON automatically when stdout isn't a
  terminal. `deadline bundle submit` always prints text, never JSON, and the
  job ID appears near the end.
- Check access first: `deadline auth status --profile P --output json`.

## Find resources

    deadline farm list --profile P               # farm IDs and their regions
    deadline queue list --profile P --farm-id F
    deadline fleet list --profile P --farm-id F
    deadline job list --profile P --farm-id F --queue-id Q    # newest first
    deadline job get "<name>" --profile P --farm-id F --queue-id Q

`job get` searches by name within ONE queue. To find a job when the queue is
unknown, run it against each queue from `deadline queue list`.

## Write a job bundle

A bundle is a DIRECTORY with `template.yaml` (the OpenJD job template) and
optionally `parameter_values.yaml` and `asset_references.yaml`.

    specificationVersion: jobtemplate-2023-09
    name: Frame Echo
    parameterDefinitions:
    - name: Frames
      type: STRING          # a range expression like "1-10" or "1-9:2,20"
      default: "1-10"
    - name: InputFile
      type: PATH
      objectType: FILE
      dataFlow: IN          # uploaded with job attachments
    - name: OutputDir
      type: PATH
      objectType: DIRECTORY
      dataFlow: OUT         # downloadable with `job download-output`
    steps:
    - name: Render
      parameterSpace:
        taskParameterDefinitions:
        - name: Frame
          type: INT
          range: "{{Param.Frames}}"
      script:
        actions:
          onRun:
            command: bash
            args: ["{{Task.File.Run}}"]
        embeddedFiles:
        - name: Run
          type: TEXT
          data: |
            set -euo pipefail
            echo "rendering frame {{Task.Param.Frame}}"
            cp "{{Param.InputFile}}" "{{Param.OutputDir}}/frame_{{Task.Param.Frame}}.txt"
    - name: Encode
      dependencies:
      - dependsOn: Render   # runs after every Render task succeeds
      script:
        actions:
          onRun:
            command: bash
            args: ["-c", "echo encoding"]

- Reference values as `{{Param.Name}}` and `{{Task.Param.Name}}`. A range
  parameter must be STRING. INT takes a single number.
- Write task scripts as embeddedFiles and quote PATH values: paths may
  contain spaces.
- `parameter_values.yaml` sets job parameters and queue environment
  parameters:

      parameterValues:
      - name: CondaPackages
        value: blender=4.2

- `deadline bundle info DIR` shows names and parameters. It does NOT
  validate the template. To validate and run it locally without AWS, install
  OpenJD's CLI (`pip install openjd-cli`), then run
  `openjd check DIR/template.yaml` and
  `openjd run DIR/template.yaml --step Render -p Frames=1-2`.
- Templates use jobtemplate-2023-09. Learn more about job bundles in the
  developer guide:
  https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html

## Submit, wait, collect

    out=$(deadline bundle submit ./bundle --yes --profile P --farm-id F \\
          --queue-id Q -p Frames=1-10) || exit 1
    job=$(printf '%s\\n' "$out" | grep -oE 'job-[0-9a-f]{32}' | tail -n 1)
    deadline job wait --profile P --farm-id F --queue-id Q --job-id "$job"
    deadline job download-output --profile P --farm-id F --queue-id Q \\
          --job-id "$job" --yes

- Always pass the parsed job ID. Submit records `defaults.job_id` only when you
  passed none of `--profile`, `--farm-id`, `--queue-id`, `--region`, or
  `--storage-profile-id`, so the default can point at an older job.
- `job wait` exit codes: 0 succeeded, 1 timed out, 2 failed, 3 canceled,
  4 suspended, 5 not compatible.
- `job download-output` writes to the output paths recorded when the job was
  submitted (remapped by storage profile), NOT the current directory. Existing
  files get a " (1)" copy unless you pass `--conflict-resolution skip` or
  `overwrite`. Run it only where writing to those paths is fine. There is no
  destination flag. To collect the files somewhere else, copy the paths it
  reports:

      deadline job download-output --profile P --farm-id F --queue-id Q \\
            --job-id "$job" --yes --output json > dl.jsonl || exit 1
      python3 -c 'import json,sys; [print(f) for l in open(sys.argv[1]) for m in [json.loads(l)] if m.get("messageType") == "summary" for f in m.get("files", [])]' dl.jsonl |
        while IFS= read -r f; do cp "$f" ./results/; done
- Check the queue's own parameters with `deadline queue paramdefs` and set them
  with `-p` or `parameter_values.yaml`.

## Diagnose a failed job

1. List the failed tasks. On a job that has already finished, `job wait` returns
   at once:

       deadline job wait --profile P --farm-id F --queue-id Q --job-id J \\
             --timeout 1 --output json

   `failedTasks` lists the stepName, stepId, taskId, and sessionId of each.
2. Read that session's log. Pass `--job-id` together with `--session-id`:

       deadline job logs --profile P --farm-id F --queue-id Q --job-id J \\
             --session-id S --limit 1000 --output verbose

3. Read the task's own output, then find the first error. Look for
   `openjd_fail:`, `Traceback`, and `exited with code: <non-zero>` in the
   "Running action" phase.

Pitfalls:
- Without `--session-id`, `job logs` shows the most recent session. That is
  often not the session that failed.
- `--limit N` returns the LAST N lines. The end of every session is cleanup,
  so raise `--limit` or use `--next-token` to see the task's output.
- Errors during "Session Cleanup" (for example `rm: cannot remove ...:
  Permission denied`) appear in most sessions and are rarely the cause.
- Quote the line that proves the cause. Hints such as "out-of-memory is the
  usual cause" are guesses printed by the script, not findings.

## Change jobs (ask first)

    deadline job requeue-tasks --job-id J --run-status failed --yes   # retry failed tasks
    deadline job cancel --job-id J --yes

These change shared farm state. Confirm with the user before you run them.
"""
