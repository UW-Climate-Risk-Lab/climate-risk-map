window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(e, ctx) {
            ctx.setProps({
                clickData: e.layer.feature
            })
        }
    }
});