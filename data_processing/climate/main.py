from src import pipeline

MODELS = [
        {
            "model": "ACCESS-CM2",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "ACCESS-ESM1-5",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "BCC-CSM2-MR",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "CanESM5",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "CMCC-CM2-SR5",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "CMCC-ESM2",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "CNRM-CM6-1",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f2",
            "use": False
        },
        {
            "model": "CNRM-ESM2-1",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f2",
            "use": False
        },
        {
            "model": "EC-Earth3-Veg-LR",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "EC-Earth3",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "FGOALS-g3",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r3i1p1f1",
            "use": False
        },
        {
            "model": "GFDL-CM4",
            "scenario": ["historical", "ssp245", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "GFDL-CM4_gr2",
            "scenario": ["historical", "ssp245", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "GFDL-ESM4",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "GISS-E2-1-G",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f2",
            "use": False
        },
        {
            "model": "HadGEM3-GC31-LL",
            "scenario": ["historical", "ssp126", "ssp245", "ssp585"],
            "ensemble_member": "r1i1p1f3",
            "use": True
        },
        {
            "model": "HadGEM3-GC31-MM",
            "scenario": ["historical", "ssp126", "ssp585"],
            "ensemble_member": "r1i1p1f3",
            "use": False
        },
        {
            "model": "INM-CM4-8",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "INM-CM5-0",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "IPSL-CM6A-LR",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "KACE-1-0-G",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "KIOST-ESM",
            "scenario": ["historical", "ssp126", "ssp245", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "MIROC-ES2L",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f2",
            "use": False
        },
        {
            "model": "MIROC6",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "MPI-ESM1-2-HR",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "MPI-ESM1-2-LR",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "MRI-ESM2-0",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "NorESM2-LM",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": True
        },
        {
            "model": "NorESM2-MM",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "TaiESM1",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f1",
            "use": False
        },
        {
            "model": "UKESM1-0-LL",
            "scenario": ["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
            "ensemble_member": "r1i1p1f2",
            "use": True
        },
    ]


def main():
    
    for model_config in MODELS:
        if model_config["use"]:
            for scenario in model_config["scenario"]:
                pipeline.main(model=model_config["model"], scenario=scenario, ensemble_member=model_config["ensemble_member"])
        else:
            continue



if __name__ == "__main__":
    main()
