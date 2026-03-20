from marshmallow import Schema, fields, validate

class AudioMetadataSchema(Schema):
    """Validation schema for audio metadata"""
    user_id = fields.Str(required=True)
    speaker_name = fields.Str(required=True, validate=validate.Length(min=1, max=100))
    language = fields.Str(required=True, validate=validate.Length(equal=2))
    gender = fields.Str(validate=validate.OneOf(['M', 'F', 'O']))
    country = fields.Str(required=True)
    state = fields.Str(required=True)
    city = fields.Str(required=True)
    age = fields.Int(required=True, validate=validate.Range(min=1, max=120))
    accent = fields.Str(validate=validate.OneOf(['Rural', 'Urban']))

def validate_audio_metadata(data):
    """Validate audio metadata"""
    schema = AudioMetadataSchema()
    return schema.load(data)
