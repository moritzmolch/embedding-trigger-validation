from law.util import DotDict
from order import Campaign, Channel, Dataset, Process
from typing import Any, Dict, List, Optional, Tuple


def create_campaign(
    name: str,
    id: int,
    label: Optional[str] = None,
    cmssw: Optional[Dict[str, Any]] = None,
    aux: Optional[Dict[str, Any]] = None,
) -> Campaign:
    """
    Create a new campaign, which can be understood as a collection of datasets corresponding to the same MC production
    campaign or data-taking era.
    """

    # `Campaign` does not have a `label` and `cmssw` member, so add it to the auxiliary data
    aux = aux or {}
    aux.update(dict(label=label, cmssw=cmssw or {}))
    aux = DotDict.wrap(aux)

    # create the campaign
    campaign = Campaign(
        name=name,
        id=id,
        aux=aux,
    )

    return campaign


def create_dataset(
    name: str,
    id: str,
    n_events: int,
    n_files: int,
    process: Process,
    tags: Optional[List[str]] = None,
    redirectors: Optional[List[str]] = None,
    lfns: Optional[List[str]] = None,
    aux: Optional[Dict[str, Any]] = None,
):
    """
    Create a dataset, which is a collection of dataset files.
    """

     # `Dataset` does not have a `redirectors` and `lfns` member, so add it to the auxiliary data
    aux = aux or {}
    aux.update(dict(
        lfns=lfns or [],
        redirectors=redirectors or [],
    ))
    aux = DotDict.wrap(aux)

    # create the dataset
    dataset = Dataset(
        name=name,
        id=id,
        processes=[process],
        n_events=n_events,
        n_files=n_files,
        tags=tags,
        aux=aux,
    )

    return dataset


def create_process(
    name: str,
    label: Optional[str],
    color: Optional[Tuple[int, int, int]],
    aux: Optional[Dict[str, Any]] = None,
) -> Process:
    """
    Create a process, which summarizes all datasets belonging to the same class of physics processes.
    """

    # set the aux dict
    aux = aux or {}

    # create the process
    process = Process(
        name=name,
        label=label,
        color1=color,
        aux=aux,
    )

    return process


def create_channel(
    name: str,
    label: Optional[str],
    triggers: Optional[List[str]],
    aux: Optional[Dict[str, Any]] = None,
) -> Process:
    """
    Create a channel, which mainly contains the channel's label and the used triggers.
    """

     # `Channel` does not have a `triggers` member, so add it to the auxiliary data
    aux = aux or {}
    aux.update(dict(triggers=triggers or []))
    aux = DotDict.wrap(aux)

    # create the process
    channel = Channel(
        name=name,
        label=label,
        aux=aux,
    )

    return channel


def check_keys(d: Dict[str, Any], required=None, optional=None):

    # get the keys of the mapping
    required_keys = set([item[0] for item in required])
    optional_keys = set([item[0] for item in optional])
    keys = set(map.keys())

    # check if all required keys are set
    required_but_not_set = keys.intersection(required_keys).difference(required_keys)
    if len(required_but_not_set) > 0:
        raise ValueError("keys {} are required but not set".format(required_but_not_set))
            
    # check if only required and optional keys are set
    unknown_keys = keys.difference(required_keys.union(optional_keys))
    if len(unknown_keys) > 0:
        raise ValueError("keys {} are unknown".format(unknown_keys))

    # check if all values of the map have the correct type if they are set
    object_types = {
        key: object_type
        for key, object_type in required.union(optional)
    }
    for k, v in d.items():
        if not isinstance(v, object_types[k]):
            raise ValueError("expected object of type '{}' for element with key '{}'".format(object_types[k], k))
