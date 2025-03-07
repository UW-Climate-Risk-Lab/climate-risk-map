import traceback
import logging
import functools
from dash import no_update

logger = logging.getLogger(__name__)

def handle_callback_error(output_count:int = None):
    """Decorator for handling errors in callbacks
    
    Args:
        output_count: Optional number of outputs from the callback
                    If None, will try to determine automatically
                    
    Returns:
        Decorator function

    -- Example usage --

    app = dash.Dash(__name__)

    app.layout = html.Div([
        dcc.Input(id='input-text', type='text', value=''),
        html.Div(id='output-text'),
        html.Div(id='output-length')
    ])

    @app.callback(
        [Output('output-text', 'children'),
        Output('output-length', 'children')],
        [Input('input-text', 'value')],
        prevent_initial_call=True
    )
    @handle_callback_error(output_count=2)
    def update_outputs(text):
        text = None
        return f'You entered: {text}', f'Length: {len(text)}'

    """
    def decorator(callback_function):
        @functools.wraps(callback_function)
        def wrapper(*args, **kwargs):
            try:
                return callback_function(*args, **kwargs)
            except Exception as e:
                # Log the error with traceback
                logger.error(f"Error in callback {callback_function.__name__}: {str(e)}")
                logger.error(traceback.format_exc())
                
                # Use provided output_count or try to determine it
                outputs = output_count
                if outputs is None:
                    return None
                return (no_update,) * outputs
                
        return wrapper
    return decorator