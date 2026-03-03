# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline.client.cli._markdown_strip import strip_markdown_for_terminal


class TestInlineLinks:
    def test_basic(self):
        assert (
            strip_markdown_for_terminal("Learn more about [jobs](https://example.com).")
            == "Learn more about jobs (https://example.com)."
        )

    def test_multiple(self):
        text = "[foo](http://a.com) and [bar](http://b.com)"
        assert strip_markdown_for_terminal(text) == "foo (http://a.com) and bar (http://b.com)"

    def test_url_with_path(self):
        text = "[docs](https://docs.aws.amazon.com/deadline-cloud/latest/userguide/farms.html)"
        result = strip_markdown_for_terminal(text)
        assert (
            result
            == "docs (https://docs.aws.amazon.com/deadline-cloud/latest/userguide/farms.html)"
        )


class TestReferenceLinks:
    def test_shorthand(self):
        text = "Work with [Deadline Cloud jobs].\n\n[Deadline Cloud jobs]: https://example.com"
        assert strip_markdown_for_terminal(text) == "Work with Deadline Cloud jobs."

    def test_explicit_ref(self):
        text = "See [the docs][ref].\n\n[ref]: https://example.com"
        assert strip_markdown_for_terminal(text) == "See the docs."

    def test_multiple_refs(self):
        text = (
            "Work with [jobs] in a [queue].\n\n"
            "[jobs]: https://example.com/jobs\n"
            "[queue]: https://example.com/queues"
        )
        assert strip_markdown_for_terminal(text) == "Work with jobs in a queue."

    def test_definitions_removed(self):
        text = "[ref]: https://example.com"
        assert strip_markdown_for_terminal(text) == ""

    def test_indented_definition(self):
        text = "Hello.\n\n    [ref]: https://example.com"
        assert strip_markdown_for_terminal(text) == "Hello."


class TestBold:
    def test_asterisks(self):
        assert strip_markdown_for_terminal("This is **important**") == "This is important"

    def test_underscores(self):
        assert strip_markdown_for_terminal("This is __important__") == "This is important"


class TestItalic:
    def test_asterisks(self):
        assert strip_markdown_for_terminal("This is *italic* text") == "This is italic text"


class TestBackticksPreserved:
    def test_backticks_untouched(self):
        text = "Run `deadline job logs` to see output"
        assert strip_markdown_for_terminal(text) == text


class TestMixed:
    def test_inline_link_and_bold(self):
        text = "Use [submit](https://example.com) to create a **new** job."
        assert (
            strip_markdown_for_terminal(text)
            == "Use submit (https://example.com) to create a new job."
        )

    def test_ref_link_and_inline_link(self):
        text = (
            "Download output from [Deadline Cloud jobs].\n\n"
            "Learn more about [attachments](https://example.com/att).\n\n"
            "[Deadline Cloud jobs]: https://example.com/jobs"
        )
        result = strip_markdown_for_terminal(text)
        assert "Download output from Deadline Cloud jobs." in result
        assert "Learn more about attachments (https://example.com/att)." in result
        assert "[Deadline Cloud jobs]:" not in result


class TestBlankLineCleanup:
    def test_triple_blank_lines_collapsed(self):
        text = "Hello.\n\n\n\nWorld."
        assert strip_markdown_for_terminal(text) == "Hello.\n\nWorld."


class TestPassthrough:
    def test_plain_text_unchanged(self):
        text = "Just plain text with no markdown."
        assert strip_markdown_for_terminal(text) == text

    def test_cli_help_block_preserved(self):
        text = "Summary.\n\n\\b\n  1. First\n  2. Second"
        assert strip_markdown_for_terminal(text) == text
