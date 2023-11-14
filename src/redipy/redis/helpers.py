"""Lua helper functions for patching lua function calls."""
from redipy.plugin import HelperFunction


class HPairlistScoresFn(HelperFunction):
    """Converts an alternating list of keys and values into a list of pairs."""
    @staticmethod
    def name() -> str:
        return "pairlist_scores"

    @staticmethod
    def args() -> str:
        return "arr"

    @staticmethod
    def body() -> str:
        return r"""
            local res = {}
            local key = nil
            for ix, value in ipairs(arr) do
                if ix % 2 == 1 then
                    key = value
                else
                    res[#res + 1] = {key, tonumber(value)}
                end
            end
            return res
        """


class HPairlistDictFn(HelperFunction):
    """Converts an alternating list of keys and values into a dictionary."""
    @staticmethod
    def name() -> str:
        return "pairlist_dict"

    @staticmethod
    def args() -> str:
        return "arr"

    @staticmethod
    def body() -> str:
        return r"""
            local res = {}
            local key = nil
            for _, value in ipairs(arr) do
                if key ~= nil then
                    res[key] = value
                    key = nil
                else
                    key = value
                end
            end
            return res
        """


class HKeyValDictFn(HelperFunction):
    """Constructs a dictionary from values and variadic keys."""
    @staticmethod
    def name() -> str:
        return "keyval_dict"

    @staticmethod
    def args() -> str:
        return "values, ..."

    @staticmethod
    def body() -> str:
        return r"""
            local res = {}
            for ix, key in ipairs(arg) do
                res[key] = values[ix] or cjson.null
            end
            return res
        """


class HNilOrIndexFn(HelperFunction):
    """Updates an index from 1-based (lua) to 0-based (python) if the value is
    not None (nil)."""
    @staticmethod
    def name() -> str:
        return "nil_or_index"

    @staticmethod
    def args() -> str:
        return "val"

    @staticmethod
    def body() -> str:
        return r"""
            if val ~= nil then
                val = val - 1
            end
            return val
        """


class HAsIntStrFn(HelperFunction):
    """Returns the integer value of a number."""
    @staticmethod
    def name() -> str:
        return "asintstr"

    @staticmethod
    def args() -> str:
        return "val"

    @staticmethod
    def body() -> str:
        return r"""
            return math.floor(val)
        """
