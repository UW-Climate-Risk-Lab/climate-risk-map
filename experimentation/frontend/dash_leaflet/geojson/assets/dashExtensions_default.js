window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, latlng, index, context) {
            const scatterIcon = L.DivIcon.extend({
                createIcon: function(oldIcon) {
                    let icon = L.DivIcon.prototype.createIcon.call(this, oldIcon);
                    icon.style.backgroundColor = this.options.color;
                    return icon;
                }
            })
            // Render a circle with the number of leaves written in the center.
            const icon = new scatterIcon({
                html: '<div style="background-color:rgba(255, 255, 255, 0);"><span>' + '</span></div>',
                className: "marker-cluster",
                iconSize: L.point(40, 40),
            });
            return L.marker(latlng, {
                icon: icon
            })
        },
        function1: function(feature, latlng) {
            const custom_icon = L.icon({
                iconUrl: `assets/power-plant.svg`,
                iconSize: [15, 15]
            });
            return L.marker(latlng, {
                icon: custom_icon
            });
        },
        function2: function(feature, latlng) {
            const custom_icon = L.icon({
                iconUrl: `assets/electric.svg`,
                iconSize: [15, 15]
            });
            return L.marker(latlng, {
                icon: custom_icon
            });
        },
        function3: function(feature, latlng) {
            const custom_icon = L.icon({
                iconUrl: `assets/black-dot.svg`,
                iconSize: [15, 15]
            });
            return L.marker(latlng, {
                icon: custom_icon
            });
        }
    }
});