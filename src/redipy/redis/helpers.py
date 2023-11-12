from redipy.plugin import HelperFunction


class HPairlistScoresFn(HelperFunction):
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


class HNilOrIndexFn(HelperFunction):
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
