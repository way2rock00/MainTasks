import { DEPLOY_LAYOUT } from './deploy-layout.config';
import { CONSTRUCT_LAYOUT } from './construct-layout.config';
import { VALIDATE_LAYOUT } from './validate-layout.config';

import { LAYOUT_DELIVER_SUB_NAV } from '../standard-constants.model';
import { ACTIVATE_LAYOUT } from './activate-layout.config';

export default {
    [LAYOUT_DELIVER_SUB_NAV.CONSTRUCT]: CONSTRUCT_LAYOUT,
    [LAYOUT_DELIVER_SUB_NAV.VALIDATE] : VALIDATE_LAYOUT,
    [LAYOUT_DELIVER_SUB_NAV.DEPLOY]   : DEPLOY_LAYOUT,
    [LAYOUT_DELIVER_SUB_NAV.ACTIVATE] : ACTIVATE_LAYOUT
}
