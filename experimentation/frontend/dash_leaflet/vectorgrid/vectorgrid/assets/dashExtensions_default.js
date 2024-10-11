window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(e) {
            console.log(e.layer.feature);
        }
    }
});