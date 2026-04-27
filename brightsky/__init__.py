import warnings

from fastapi.exceptions import FastAPIDeprecationWarning


# FastAPI has deprecated ORJSONResponse in favour of just returning a
# Pydantic response model. While we're already using response models, we're
# for the time being still relying on directly returning an ORJSONResponse:
# 1. We're using a custom ORJSONReponse subclass for encoding radar data,
# 2. There are some inconsistencies around nullable fields in the response
#    models (which is why we're using `json_schema_extra`).
warnings.filterwarnings(
    'ignore',
    category=FastAPIDeprecationWarning,
    message=r'ORJSONResponse',
)


__version__ = '2.2.9'
