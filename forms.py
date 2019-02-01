from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

class SelectPlatformForm(FlaskForm):
    platform = StringField('Platform', validators=[DataRequired()])
    submit = SubmitField('Next')

class SelectFunctionForm(FlaskForm):
    function = StringField('Function', validators=[DataRequired()])
    submit = SubmitField('Next')