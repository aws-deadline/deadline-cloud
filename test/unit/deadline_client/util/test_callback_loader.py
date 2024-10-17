import os

from deadline.client import util


def _asset_not_raises(fn, *args, **kwargs):
    try:
        ret = fn(*args, **kwargs)
        return ret
    except Exception as err:
        assert "Did raise unexpected exception. {}".format(str(err))


def _asset_raises(fn, exception_type, *args, **kwargs):
    try:
        fn(*args, **kwargs)
        assert False is True
    except exception_type:
        return True
    except Exception as err:
        assert "Did not raise expected {} type. {}".format(exception_type, str(err))


def test_loading_valid_config(fresh_deadline_config):
    on_pre_submit_callback = _asset_not_raises(
        util.callback_loader.import_module_function,
        module_path=os.path.join(os.path.dirname(__file__), "_callback_loader_valid_config.py"),
        module_name="test_loading_valid_config",
        function_name="on_pre_submit_callback",
    )

    assert util.callback_loader.validate_function_signature(function=on_pre_submit_callback)

    on_post_submit_callback = _asset_not_raises(
        util.callback_loader.import_module_function,
        module_path=os.path.join(os.path.dirname(__file__), "_callback_loader_valid_config.py"),
        module_name="test_loading_valid_config",
        function_name="on_post_submit_callback",
    )

    assert util.callback_loader.validate_function_signature(function=on_post_submit_callback)


def test_loading_invalid_signature_config(fresh_deadline_config):
    on_pre_submit_callback = _asset_not_raises(
        util.callback_loader.import_module_function,
        module_path=os.path.join(
            os.path.dirname(__file__), "_callback_loader_invalid_signature_config.py"
        ),
        module_name="test_loading_invalid_signature_config",
        function_name="on_pre_submit_callback",
    )

    valid_signature_pre = util.callback_loader.validate_function_signature(
        function=on_pre_submit_callback
    )
    assert valid_signature_pre is False

    on_post_submit_callback = _asset_not_raises(
        util.callback_loader.import_module_function,
        module_path=os.path.join(
            os.path.dirname(__file__), "_callback_loader_invalid_signature_config.py"
        ),
        module_name="test_loading_invalid_signature_config",
        function_name="on_post_submit_callback",
    )

    valid_signature_post = util.callback_loader.validate_function_signature(
        function=on_post_submit_callback
    )
    assert valid_signature_post is False


def test_loading_missing_function_config(fresh_deadline_config):
    _asset_raises(
        util.callback_loader.import_module_function,
        exception_type=AttributeError,
        module_path=os.path.join(
            os.path.dirname(__file__), "_callback_loader_missing_function_config.py"
        ),
        module_name="test_loading_missing_function_config",
        function_name="on_pre_submit_callback",
    )

    _asset_raises(
        util.callback_loader.import_module_function,
        exception_type=AttributeError,
        module_path=os.path.join(
            os.path.dirname(__file__), "_callback_loader_missing_function_config.py"
        ),
        module_name="test_loading_missing_function_config",
        function_name="on_post_submit_callback",
    )


def test_loading_invalid_syntax_config(fresh_deadline_config):
    _asset_raises(
        util.callback_loader.import_module_function,
        exception_type=SyntaxError,
        module_path=os.path.join(
            os.path.dirname(__file__), "_callback_loader_invalid_syntax_config.py"
        ),
        module_name="test_loading_invalid_syntax_config",
        function_name="on_pre_submit_callback",
    )

    _asset_raises(
        util.callback_loader.import_module_function,
        exception_type=SyntaxError,
        module_path=os.path.join(
            os.path.dirname(__file__), "_callback_loader_invalid_syntax_config.py"
        ),
        module_name="test_loading_invalid_syntax_config",
        function_name="on_post_submit_callback",
    )
