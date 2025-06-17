import argparse
import boto3
import json
import time
import logging
import re
from botocore.exceptions import ClientError

from prompts import PROMPTS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _sanitize_for_iam_name(name):
    # Allowed characters for IAM role/policy names: alphanumeric, +=,.@_-
    # Replace any character NOT in the allowed set with an underscore
    return re.sub(r'[^a-zA-Z0-9+=,.@_-]', '_', name)

def delete_existing_agent_and_role(bedrock_agent_client, iam_client, agent_name, role_name):
    """Deletes an existing Bedrock agent and its associated IAM role if they exist."""
    try:
        # Find and delete the agent
        agents_response = bedrock_agent_client.list_agents()
        agent_id = None
        for agent_summary in agents_response.get('agentSummaries', []):
            if agent_summary['agentName'] == agent_name:
                agent_id = agent_summary['agentId']
                break

        if agent_id:
            logging.info(f"Deleting existing agent '{agent_name}' (ID: {agent_id})...")
            
            # You may need to delete aliases first if they exist
            aliases = bedrock_agent_client.list_agent_aliases(agentId=agent_id).get('agentAliasSummaries', [])
            for alias in aliases:
                logging.info(f"Deleting alias '{alias['agentAliasName']}'...")
                bedrock_agent_client.delete_agent_alias(agentId=agent_id, agentAliasId=alias['agentAliasId'])
                time.sleep(2) # Allow time for alias deletion

            bedrock_agent_client.delete_agent(agentId=agent_id)
            logging.info("Agent deletion initiated. Waiting for it to complete...")
            
            # Wait for agent deletion
            while True:
                try:
                    bedrock_agent_client.get_agent(agentId=agent_id)
                    time.sleep(5)
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logging.info("Agent successfully deleted.")
                        break
                    else:
                        raise
        else:
            logging.info(f"No agent named '{agent_name}' found.")

    except ClientError as e:
        logging.error(f"An error occurred while deleting the agent: {e}")
        return # Stop if agent deletion fails

    try:
        # Find and delete the IAM role and its policies
        logging.info(f"Checking for existing IAM role '{role_name}'...")
        iam_client.get_role(RoleName=role_name) # Check if role exists
        
        logging.info(f"Deleting IAM role '{role_name}' and its policies...")
        
        # Detach and delete inline policies
        policy_names = iam_client.list_role_policies(RoleName=role_name).get('PolicyNames', [])
        for policy_name in policy_names:
            logging.info(f"Deleting inline policy '{policy_name}' from role '{role_name}'...")
            iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            
        iam_client.delete_role(RoleName=role_name)
        logging.info(f"IAM role '{role_name}' deleted.")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            logging.info(f"No IAM role named '{role_name}' found.")
        else:
            logging.error(f"An error occurred while deleting the IAM role: {e}")


def main():
    """Main function to create the Bedrock agent."""
    parser = argparse.ArgumentParser(description="Create a climate risk analysis AI agent in AWS Bedrock.")
    parser.add_argument("--hazard", type=str, required=True, choices=PROMPTS.keys(), help="The hazard type for the agent's specialization (e.g., 'wildfire', 'flood').")
    parser.add_argument("--foundational_model", type=str, default="anthropic.claude-3-5-sonnet-20240620-v1:0", help="The foundational model ARN for the agent.")
    parser.add_argument("--region_name", type=str, default="us-west-2", help="The AWS region to create the agent in.")
    args = parser.parse_args()

    sts = boto3.client('sts')
    caller_identity = sts.get_caller_identity()
    ACCOUNT_ID = str(caller_identity['Account'])
    REGION_NAME = args.region_name
    FOUNDATIONAL_MODEL = args.foundational_model
    HAZARD = args.hazard
    
    # We use a simplified model name for resource naming to avoid special characters
    # Sanitize the foundational model name part
    if '.' in FOUNDATIONAL_MODEL:
        raw_model_part = FOUNDATIONAL_MODEL.split('.', 1)[1]
    else:
        raw_model_part = FOUNDATIONAL_MODEL # Fallback if no dot is present
    model_name_safe = _sanitize_for_iam_name(raw_model_part)

    # Sanitize the hazard name
    hazard_name_safe = _sanitize_for_iam_name(HAZARD)

    AGENT_NAME = f'climate-risk-map-ai-agent-{hazard_name_safe}-{model_name_safe}'
    
    # Sanitize IAM role and policy names to adhere to AWS naming conventions
    IAM_ROLE_NAME = _sanitize_for_iam_name(f'{AGENT_NAME}-role')[:63]
    MODEL_POLICY_NAME = _sanitize_for_iam_name(f'{AGENT_NAME}-model-policy')
    INFERENCE_POLICY_NAME = _sanitize_for_iam_name(f'{AGENT_NAME}-inference-policy')
    
    INFERENCE_PROFILE = f"us.{FOUNDATIONAL_MODEL}"
    
    INSTRUCTION = PROMPTS[HAZARD]['instruction_prompt']

    TRUST_POLICY = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "bedrock.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {"aws:SourceAccount": ACCOUNT_ID},
                    "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock:{REGION_NAME}:{ACCOUNT_ID}:agent/*"}
                }
            }
        ]
    }
    MODEL_POLICY = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["bedrock:InvokeModel"],
                "Resource": [f"arn:aws:bedrock:{REGION_NAME}::foundation-model/{FOUNDATIONAL_MODEL}"]
            }
        ]
    }
    INFERENCE_PROFILE_POLICY = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                    "bedrock:GetInferenceProfile",
                    "bedrock:GetFoundationModel"
                ],
                "Resource": [
                    f"arn:aws:bedrock:us-west-2:{ACCOUNT_ID}:inference-profile/{INFERENCE_PROFILE}",
                    f"arn:aws:bedrock:*::foundation-model/{FOUNDATIONAL_MODEL}"
                ]
            }
        ]
    }
    
    bedrock_agent = boto3.client(service_name='bedrock-agent', region_name=REGION_NAME)
    iam = boto3.client('iam')
    
    # Clean up existing resources before creation
    delete_existing_agent_and_role(bedrock_agent, iam, AGENT_NAME, IAM_ROLE_NAME)
    
    logging.info("Creating new IAM policy and role...")
    try:
        role = iam.create_role(
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY)
        )
        iam.put_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyName=MODEL_POLICY_NAME,
            PolicyDocument=json.dumps(MODEL_POLICY)
        )
        iam.put_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyName=INFERENCE_POLICY_NAME,
            PolicyDocument=json.dumps(INFERENCE_PROFILE_POLICY)
        )
        roleArn = role['Role']['Arn']
        logging.info(f"IAM Role '{IAM_ROLE_NAME}' created successfully.")
    except ClientError as e:
        logging.error(f"Failed to create IAM role: {e}")
        return

    logging.info("Waiting for IAM propagation...")
    time.sleep(10)

    logging.info(f"Creating the agent '{AGENT_NAME}'...")
    try:
        response = bedrock_agent.create_agent(
            agentName=AGENT_NAME,
            foundationModel=INFERENCE_PROFILE,
            instruction=INSTRUCTION,
            agentResourceRoleArn=roleArn,
        )
        agentId = response['agent']['agentId']
        logging.info(f"Agent '{AGENT_NAME}' created with ID: {agentId}")
    except ClientError as e:
        logging.error(f"Failed to create agent: {e}")
        return

    # Agent creation and preparation flow
    for status_check in ["NOT_PREPARED", "PREPARED"]:
        if status_check == "NOT_PREPARED":
            logging.info("Waiting for agent status of 'NOT_PREPARED'...")
        else: # PREPARED
             logging.info("Preparing the agent...")
             bedrock_agent.prepare_agent(agentId=agentId)
             logging.info("Waiting for agent status of 'PREPARED'...")
        
        while True:
            response = bedrock_agent.get_agent(agentId=agentId)
            agentStatus = response['agent']['agentStatus']
            logging.info(f"Agent status: {agentStatus}")
            if agentStatus == status_check:
                break
            time.sleep(3)

        if status_check == "NOT_PREPARED":
            # Configure code interpreter
            logging.info("Creating and enabling Code Interpreter action group...")
            bedrock_agent.create_agent_action_group(
                actionGroupName='CodeInterpreterAction',
                actionGroupState='ENABLED',
                agentId=agentId,
                agentVersion='DRAFT',
                parentActionGroupSignature='AMAZON.CodeInterpreter'
            )
    
    # Create an alias for the prepared agent
    alias_name = 'live'
    logging.info(f"Creating agent alias '{alias_name}'...")
    try:
        response = bedrock_agent.create_agent_alias(
            agentAliasName=alias_name,
            agentId=agentId
        )
        agentAliasId = response['agentAlias']['agentAliasId']
        
        while True:
            response = bedrock_agent.get_agent_alias(agentId=agentId, agentAliasId=agentAliasId)
            aliasStatus = response['agentAlias']['agentAliasStatus']
            logging.info(f"Agent alias status: {aliasStatus}")
            if aliasStatus == 'PREPARED':
                break
            time.sleep(3)
        
        logging.info("Agent setup complete.")
        logging.info(f"Agent ID: {agentId}")
        logging.info(f"Agent Alias ID ('{alias_name}'): {agentAliasId}")

    except ClientError as e:
        logging.error(f"Failed to create or prepare agent alias: {e}")

if __name__ == "__main__":
    main()
