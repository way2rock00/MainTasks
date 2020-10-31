import { 
    LAYOUT_TYPE, 
    LAYOUT_DELIVER_SUB_NAV, 
    LAYOUT_IMAGINE_SUB_NAV, 
    LAYOUT_INSIGHTS_SUB_NAV, 
    LAYOUT_RUN_SUB_NAV 
} from './layout/standard/standard-constants.model';

export {
    LAYOUT_TYPE, 
    LAYOUT_DELIVER_SUB_NAV, 
    LAYOUT_IMAGINE_SUB_NAV, 
    LAYOUT_INSIGHTS_SUB_NAV, 
    LAYOUT_RUN_SUB_NAV 
} 

import DELIVER_LAYOUT_CONFIG from './layout/standard/deliver';
import IMAGINE_LAYOUT_CONFIG from './layout/standard/imagine';
import INSIGHTS_LAYOUT_CONFIG from './layout/standard/insights';
import RUN_LAYOUT_CONFIG from './layout/standard/run';


/* -- FINAL CONFIG OBJECT -- */
export const LAYOUT_CONFIGURATION = {
    [LAYOUT_TYPE.INSIGHTS]: INSIGHTS_LAYOUT_CONFIG,
    [LAYOUT_TYPE.IMAGINE] : IMAGINE_LAYOUT_CONFIG,
    [LAYOUT_TYPE.DELIVER] : DELIVER_LAYOUT_CONFIG,
    [LAYOUT_TYPE.RUN]     : RUN_LAYOUT_CONFIG,
};
