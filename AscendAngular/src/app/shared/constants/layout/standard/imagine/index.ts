import { LAUNCH_LAYOUT } from './launch-layout.config';
import { PURPOSE_LAYTOUT } from './purpose-layout.config';
import { ARCHITECT_LAYOUT } from './architect-layout.config';
import { DESIGN_LAYTOUT } from './design-layout.config';
import { SUSTAINMENT_LAYTOUT } from './sustainment-layout.config'
import { LAYOUT_IMAGINE_SUB_NAV } from '../standard-constants.model';

export default {
    [LAYOUT_IMAGINE_SUB_NAV.ARCHITECT]  : ARCHITECT_LAYOUT,
    [LAYOUT_IMAGINE_SUB_NAV.DESIGN]     : DESIGN_LAYTOUT,
    [LAYOUT_IMAGINE_SUB_NAV.SUSTAINMENT]: SUSTAINMENT_LAYTOUT,
    [LAYOUT_IMAGINE_SUB_NAV.PURPOSE]    : PURPOSE_LAYTOUT,
    [LAYOUT_IMAGINE_SUB_NAV.LAUNCH]     : LAUNCH_LAYOUT
}
