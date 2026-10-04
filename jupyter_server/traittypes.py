"""项目内部接口说明。"""

import inspect
from ast import literal_eval

from traitlets import Any, ClassBasedTraitType, TraitError, Undefined
from traitlets.utils.descriptions import describe


class TypeFromClasses(ClassBasedTraitType):  # type:ignore[type-arg]
    """项目内部接口说明。"""

    default_value: Any

    def __init__(self, default_value=Undefined, klasses=None, **kwargs):
        """项目内部接口说明。"""
        if default_value is Undefined:
            new_default_value = object if (klasses is None) else klasses
        else:
            new_default_value = default_value

        if klasses is None:
            if (default_value is None) or (default_value is Undefined):
                klasses = [object]
            else:
                klasses = [default_value]

        # OneOfType requires a list of klasses to be specified (different than Type).
        if not isinstance(klasses, (list, tuple, set)):
            msg = "`klasses` must be a list of class names (type is str) or classes."
            raise TraitError(msg)

        for klass in klasses:
            if not (inspect.isclass(klass) or isinstance(klass, str)):
                msg = "A OneOfType trait must specify a list of classes."
                raise TraitError(msg)

        # Store classes.
        self.klasses = klasses

        super().__init__(new_default_value, **kwargs)

    def subclass_from_klasses(self, value):
        """项目内部接口说明。"""
        return any(issubclass(value, klass) for klass in self.importable_klasses)

    def validate(self, obj, value):
        """项目内部接口说明。"""
        if isinstance(value, str):
            try:
                value = self._resolve_string(value)
            except ImportError as e:
                emsg = (
                    f"The '{self.name}' trait of {obj} instance must be a type, but "
                    f"{value!r} could not be imported"
                )
                raise TraitError(emsg) from e
        try:
            if self.subclass_from_klasses(value):
                return value
        except Exception:
            pass

        self.error(obj, value)

    def info(self):
        """项目内部接口说明。"""
        result = "a subclass of "
        for klass in self.klasses:
            if not isinstance(klass, str):
                klass = klass.__module__ + "." + klass.__name__  # noqa: PLW2901
            result += f"{klass} or "
        # Strip the last "or"
        result = result.strip(" or ")  # noqa: B005
        if self.allow_none:
            return result + " or None"
        return result

    def instance_init(self, obj):
        """项目内部接口说明。"""
        self._resolve_classes()
        super().instance_init(obj)

    def _resolve_classes(self):
        """项目内部接口说明。"""
        self.importable_klasses = []
        for klass in self.klasses:
            if isinstance(klass, str):
                # Try importing the classes to compare. Silently, ignore if not importable.
                try:
                    klass = self._resolve_string(klass)  # noqa: PLW2901
                    self.importable_klasses.append(klass)
                except Exception:
                    pass
            else:
                self.importable_klasses.append(klass)

        if isinstance(self.default_value, str):
            self.default_value = self._resolve_string(self.default_value)  # type:ignore[arg-type]

    def default_value_repr(self):
        """项目内部接口说明。"""
        value = self.default_value
        if isinstance(value, str):
            return repr(value)
        else:
            return repr(f"{value.__module__}.{value.__name__}")


class InstanceFromClasses(ClassBasedTraitType):  # type:ignore[type-arg]
    """项目内部接口说明。"""

    def __init__(self, klasses=None, args=None, kw=None, **kwargs):
        """项目内部接口说明。"""
        # If class
        if klasses is None:  # noqa: SIM114
            self.klasses = klasses
        # Verify all elements are either classes or strings.
        elif all(inspect.isclass(k) or isinstance(k, str) for k in klasses):
            self.klasses = klasses
        else:
            raise TraitError(
                "The klasses attribute must be a list of class names or classes not: %r" % klasses
            )

        if (kw is not None) and not isinstance(kw, dict):
            msg = "The 'kw' argument must be a dict or None."
            raise TraitError(msg)
        if (args is not None) and not isinstance(args, tuple):
            msg = "The 'args' argument must be a tuple or None."
            raise TraitError(msg)

        self.default_args = args
        self.default_kwargs = kw

        super().__init__(**kwargs)

    def instance_from_importable_klasses(self, value):
        """项目内部接口说明。"""
        return any(isinstance(value, klass) for klass in self.importable_klasses)

    def validate(self, obj, value):
        """项目内部接口说明。"""
        if self.instance_from_importable_klasses(value):
            return value
        else:
            self.error(obj, value)

    def info(self):
        """项目内部接口说明。"""
        result = "an instance of "
        assert self.klasses is not None
        for klass in self.klasses:
            if isinstance(klass, str):
                result += klass
            else:
                result += describe("a", klass)
            result += " or "
        result = result.strip(" or ")  # noqa: B005
        if self.allow_none:
            result += " or None"
        return result

    def instance_init(self, obj):
        """项目内部接口说明。"""
        self._resolve_classes()
        super().instance_init(obj)

    def _resolve_classes(self):
        """项目内部接口说明。"""
        self.importable_klasses = []
        assert self.klasses is not None
        for klass in self.klasses:
            if isinstance(klass, str):
                # Try importing the classes to compare. Silently, ignore if not importable.
                try:
                    klass = self._resolve_string(klass)  # noqa: PLW2901
                    self.importable_klasses.append(klass)
                except Exception:
                    pass
            else:
                self.importable_klasses.append(klass)

    def make_dynamic_default(self):
        """项目内部接口说明。"""
        if (self.default_args is None) and (self.default_kwargs is None):
            return None
        return self.klass(  # type:ignore[attr-defined]
            *(self.default_args or ()), **(self.default_kwargs or {})
        )

    def default_value_repr(self):
        """项目内部接口说明。"""
        return repr(self.make_dynamic_default())

    def from_string(self, s):
        """项目内部接口说明。"""
        return literal_eval(s)
