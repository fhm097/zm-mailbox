/*
 * ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1
 * 
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 ("License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.zimbra.com/license
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is: Zimbra Collaboration Suite Server.
 * 
 * The Initial Developer of the Original Code is Zimbra, Inc.
 * Portions created by Zimbra are Copyright (C) 2004, 2005, 2006 Zimbra, Inc.
 * All Rights Reserved.
 * 
 * Contributor(s): 
 * 
 * ***** END LICENSE BLOCK *****
 */

/*
 * Created on Aug 31, 2004
 */
package com.zimbra.cs.service.mail;

import java.util.Iterator;
import java.util.Map;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.soap.MailConstants;
import com.zimbra.cs.mailbox.Mailbox;
import com.zimbra.cs.mailbox.Mountpoint;
import com.zimbra.cs.mailbox.Mailbox.OperationContext;
import com.zimbra.cs.operation.GetFolderTreeOperation;
import com.zimbra.cs.operation.GetFolderTreeOperation.FolderNode;
import com.zimbra.cs.operation.Operation.Requester;
import com.zimbra.cs.service.util.ItemId;
import com.zimbra.cs.service.util.ItemIdFormatter;
import com.zimbra.cs.session.Session;
import com.zimbra.common.soap.Element;
import com.zimbra.common.soap.SoapFaultException;
import com.zimbra.soap.ZimbraSoapContext;

/**
 * @author dkarp
 */
public class GetFolder extends MailDocumentHandler {

    private static final String[] TARGET_FOLDER_PATH = new String[] { MailConstants.E_FOLDER, MailConstants.A_FOLDER };
    private static final String[] RESPONSE_ITEM_PATH = new String[] { MailConstants.E_FOLDER };
    protected String[] getProxiedIdPath(Element request)     { return TARGET_FOLDER_PATH; }
    protected boolean checkMountpointProxy(Element request)  { return false; }
    protected String[] getResponseItemPath()  { return RESPONSE_ITEM_PATH; }

    static final String DEFAULT_FOLDER_ID = "" + Mailbox.ID_FOLDER_USER_ROOT;

	public Element handle(Element request, Map<String, Object> context) throws ServiceException, SoapFaultException {
		ZimbraSoapContext lc = getZimbraSoapContext(context);
		Mailbox mbox = getRequestedMailbox(lc);
		Mailbox.OperationContext octxt = lc.getOperationContext();
        ItemIdFormatter ifmt = new ItemIdFormatter(lc);
		Session session = getSession(context);

		String parentId = DEFAULT_FOLDER_ID;
		Element eFolder = request.getOptionalElement(MailConstants.E_FOLDER);
		if (eFolder != null) {
            String path = eFolder.getAttribute(MailConstants.A_PATH, null);
            if (path != null)
                parentId = mbox.getFolderByPath(octxt, path).getId() + "";
            else
                parentId = eFolder.getAttribute(MailConstants.A_FOLDER, DEFAULT_FOLDER_ID);
        }
		ItemId iid = new ItemId(parentId, lc);

        boolean visible = request.getAttributeBool(MailConstants.A_VISIBLE, false);

		Element response = lc.createElement(MailConstants.GET_FOLDER_RESPONSE);
		
		GetFolderTreeOperation op = new GetFolderTreeOperation(session, octxt, mbox, Requester.SOAP, iid, visible);
		op.schedule();
		FolderNode rootnode = op.getResult();

		Element folderRoot = encodeFolderNode(ifmt, octxt, response, rootnode);
		if (rootnode.mVisible && rootnode.mFolder instanceof Mountpoint) {
			handleMountpoint(request, context, iid, (Mountpoint) rootnode.mFolder, folderRoot);			
		}
		
		return response;
	}

	public static Element encodeFolderNode(ItemIdFormatter ifmt, OperationContext octxt, Element parent, FolderNode node) {
		Element eFolder;
        if (node.mVisible)
            eFolder = ToXML.encodeFolder(parent, ifmt, octxt, node.mFolder);
        else
            eFolder = parent.addElement(MailConstants.E_FOLDER).addAttribute(MailConstants.A_ID, node.mId).addAttribute(MailConstants.A_NAME, node.mName);

		for (FolderNode subNode : node.mSubfolders)
			encodeFolderNode(ifmt, octxt, eFolder, subNode);

		return eFolder;
	}

//	public static Element handleFolder(Mailbox mbox, Folder folder, Element response, ZimbraSoapContext lc, OperationContext octxt)
//    throws ServiceException {
//		Element respFolder = ToXML.encodeFolder(response, lc, folder);
//
//        for (Folder subfolder : folder.getSubfolders(octxt))
//        	if (subfolder != null)
//       		handleFolder(mbox, subfolder, respFolder, lc, octxt);
//       return respFolder;
//	}

//	public static Element handleFolder(Mailbox mbox, Folder folder, Element response, ZimbraSoapContext lc, OperationContext octxt)
//	throws ServiceException {
//		Element respFolder = ToXML.encodeFolder(response, lc, folder);
//		
//		List subfolders = folder.getSubfolders(octxt);
//		if (subfolders != null)
//			for (Iterator it = subfolders.iterator(); it.hasNext(); ) {
//				Folder subfolder = (Folder) it.next();
//				if (subfolder != null)
//					handleFolder(mbox, subfolder, respFolder, lc, octxt);
//			}
//		return respFolder;
//	}
	
	private void handleMountpoint(Element request, Map<String, Object> context, ItemId iidLocal, Mountpoint mpt, Element eRoot)
	throws ServiceException, SoapFaultException {
		ItemId iidRemote = new ItemId(mpt.getOwnerId(), mpt.getRemoteId());
		Element proxied = proxyRequest(request, context, iidLocal, iidRemote);
        // return the children of the remote folder as children of the mountpoint
		proxied = proxied.getOptionalElement(MailConstants.E_FOLDER);
		if (proxied != null) {
			eRoot.addAttribute(MailConstants.A_REST_URL, proxied.getAttribute(MailConstants.A_REST_URL, null));
			eRoot.addAttribute(MailConstants.A_RIGHTS, proxied.getAttribute(MailConstants.A_RIGHTS, null));
			for (Iterator it = proxied.elementIterator(); it.hasNext(); ) {
				Element eRemote = (Element) it.next();
				// skip the <acl> element, if any
				if (!eRemote.getName().equals(MailConstants.E_ACL))
					eRoot.addElement(eRemote.detach());
			}
		}
	}
}
